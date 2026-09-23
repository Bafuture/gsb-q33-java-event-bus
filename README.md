# In-Process Event Bus

一个进程内发布/订阅事件总线，用于解耦业务流程中「状态变更 → 多个下游动作」的硬编码调用。
新增下游模块只需 `subscribe`，主流程无需改动。

## 快速开始

```java
EventBusConfig config = EventBusConfig.builder()
        .dispatchMode(DispatchMode.ASYNC)      // 默认分发模式（也可每个订阅单独指定）
        .workerThreads(4)                      // 自研线程池的工作线程数
        .queueCapacity(1024)                   // 每个工作线程的队列容量
        .rejectionPolicy(RejectionPolicy.ABORT)// 队列满时的拒绝策略
        .maxRetries(3)                         // 投递失败后的重试次数，0 表示不重试
        .retryBackoff(Duration.ofMillis(50))
        .transactionContext(myTxContext)       // 可选：事务同步 SPI
        .build();

DefaultEventBus bus = new DefaultEventBus(config);

// 订阅父类事件，子类事件也会收到
bus.subscribe(OrderEvent.class, event -> inventory.reserve(event));
bus.subscribe(OrderEvent.class, DispatchMode.ASYNC, event -> notifier.push(event));

bus.publish(new OrderCreated("o-1"), "order-o-1");   // 第二个参数是聚合键
bus.publishAfterCommit(new OrderCreated("o-2"), "order-o-2"); // 事务提交后才发出

bus.close();
```

构建与测试：`mvn -q verify`（或 `./mvnw -q verify`）

## 类型匹配规则

订阅类型为 `T` 的监听器会收到运行时类型满足 `T.isAssignableFrom(event.getClass())` 的事件，即：

- 订阅**父类**，能收到其所有**子类**的事件；
- 订阅**接口**，能收到所有**实现类**的事件；
- 订阅具体子类，**不会**收到父类或兄弟类的事件；
- 订阅 `Object.class` 会收到所有事件；
- 匹配基于事件的运行时类，不考虑泛型参数；`null` 事件直接抛 `NullPointerException`。

对应测试：`TypeMatchingTest`。

## 分发模式与线程池

- `SYNC`：监听器在发布线程上执行，`publish` 返回时所有同步监听器已执行完毕。
- `ASYNC`：投递任务进入总线内置的自研线程池（`EventExecutor`）。

线程池结构：固定数量的工作线程，**每个线程一个独立的有界队列**（容量 = `queueCapacity`）。
任务按聚合键路由（见下节）。队列满时按 `RejectionPolicy` 处理：

- `ABORT`：向发布者抛 `EventRejectedException`；
- `CALLER_RUNS`：在发布线程上直接执行；
- `DISCARD`：静默丢弃。

对应测试：`DispatchModeTest`、`ExecutorRejectionTest`。

## 顺序保证

**保证范围**：发布时指定了聚合键（`publish(event, key)`）的事件，对**每一个订阅者**而言，
相同聚合键的事件严格按发布顺序被消费。实现方式：`hash(aggregateKey) % workerCount` 把同一键的
任务固定路由到同一个工作线程的 FIFO 队列，由单线程依次执行。

- 不同聚合键之间允许并行（不同键可能落在不同工作线程上）；
- 不指定聚合键（`key == null`）时任务轮询分发，不提供顺序保证；
- 同一键被多个线程并发发布时，「发布顺序」以任务入队顺序为准；
- 顺序保证是**按订阅者**的：同一事件对不同订阅者的投递相互独立。

对应测试：`OrderingTest`（单键 500 个事件保序、8 键交错各自保序、双键并行 rendezvous 证明）。

## 异常隔离与重试语义

- 每个监听器的调用都被独立 try/catch 包裹：一个订阅者抛异常**不影响**其他订阅者，
  也不会抛回发布者（同步模式同样如此）。
- 每次失败尝试都会记录为 `DeliveryFailure`（事件、聚合键、订阅者、异常、第几次尝试、时间），
  通过 `bus.failures()` 查询，同时以 `System.Logger` 输出告警日志。
- 重试：**在同一个投递任务内**按 `retryBackoff` 间隔原地重试，最多 `maxRetries` 次；
  `maxRetries = 0`（默认）表示不重试。异步模式下重试发生在该键对应的工作线程上，
  因此重试期间同键后续事件会排队等待（保序的必然代价）。
- 首次尝试 + `maxRetries` 次重试全部失败后，事件进入死信队列。

对应测试：`ExceptionIsolationTest`。

## 死信队列

重试耗尽后的事件进入 `DeadLetterQueue`（默认 `InMemoryDeadLetterQueue`）：

```java
List<DeadLetter> all = bus.deadLetters().list();   // 按进入时间排序
Optional<DeadLetter> one = bus.deadLetters().find(id);
boolean ok = bus.deadLetters().republish(id);      // 手动重投：移出队列并按原聚合键重新发布
```

`DeadLetter` 携带原始事件、聚合键、失败的订阅者、异常、总尝试次数和进入时间。

对应测试：`DeadLetterQueueTest`。

## 事务同步

`publishAfterCommit(event, key)` 通过 `TransactionContext` SPI 对接事务管理器：

- 有活跃事务：注册一个 `TransactionSynchronization`，**提交后才真正发布**；回滚则事件被丢弃；
- 无活跃事务（或未配置 `TransactionContext`）：立即发布。

对接 Spring 的示例适配器（需自行引入 Spring 依赖，本工程不依赖 Spring）：

```java
public class SpringTransactionContext implements TransactionContext {
    public boolean isTransactionActive() {
        return TransactionSynchronizationManager.isSynchronizationActive();
    }
    public void registerSynchronization(TransactionSynchronization sync) {
        TransactionSynchronizationManager.registerSynchronization(
            new org.springframework.transaction.support.TransactionSynchronization() {
                public void afterCommit() { sync.afterCommit(); }
                public void afterCompletion(int status) {
                    if (status == STATUS_ROLLED_BACK) sync.afterRollback();
                }
            });
    }
}
```

对应测试：`TransactionSyncTest`（提交后发出、回滚丢弃、无事务立即发出）。

## 已知限制

- **仅进程内**：无持久化、无跨进程/跨机器分发；JVM 退出时队列中未消费的事件和死信全部丢失。
- **死信队列在内存中**：重启后不可恢复；如需持久化请自行替换 `DeadLetterQueue` 实现。
- **重投是尽力而为**：`republish` 会重新走完整投递流程，若监听器仍失败会再次进入死信队列（生成新的 id）。
- **重试会阻塞同键后续事件**：异步模式下重试在工作线程内原地进行，同一聚合键的后续事件需等待。
- **`CALLER_RUNS` 的副作用**：队列满时监听器会在发布线程上执行，可能阻塞发布者。
- **关闭语义**：`close()` 中断工作线程，已入队未执行的任务不保证完成；发布方应先停止再关闭。
- **顺序保证不含无键事件**：`publish(event)` 不带聚合键时按轮询分发到各工作线程，无顺序保证。
