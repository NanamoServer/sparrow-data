# Sparrow Data

[English](./README.md) · **简体中文**

面向 Minecraft 服务器集群的实体数据层。数据挂在实体上（玩家、公会，任何有 ID 的东西），通过带类型的键读写，每个键属于两种权威模型之一：

- **内存键**：数据在当前持有这个实体的那台服务器的内存里，读写就是普通的内存调用，定时落库，`offline` 时再落一次。
- **DB 键**：数据以数据库为准，每次写入都直接落到存储上，用原子语句或 CAS 循环完成，所以任何服务器随时都能写。

其他服务器通过快照读这两种数据，快照靠 Redis 上的失效消息刷新。存储内置 MySQL、MariaDB、PostgreSQL 三种实现，其他存储实现 `EntityStorage` 即可。

## 引入

```kotlin
repositories {
    maven("https://repo.momirealms.net/snapshots")
}

dependencies {
    implementation("net.momirealms:sparrow-data:0.1")
}
```

需要 Java 21。下面这些依赖是 `compileOnly`，由宿主提供：

| 依赖 | 使用的版本 |
|---|---|
| `net.momirealms:sparrow-redis-message-broker` | 0.0.7 |
| `io.netty:netty-buffer` | 4.2.7.Final |
| `com.github.ben-manes.caffeine:caffeine` | 3.2.3 |
| `org.slf4j:slf4j-api` | 2.0.17 |

## 初始化

```java
EntityStorage storage = new MysqlEntityStorage(dataSource, "sparrow_"); // MariaDbEntityStorage / PostgresEntityStorage

DataService data = SparrowDataService.builder()
        .storage(storage)
        .executor(executor)                    // 所有阻塞的存储调用都在这里执行
        .messageBroker(broker)
        .saveInterval(Duration.ofMinutes(5))   // 持有中实体的定时落库间隔
        .snapshotExpiry(Duration.ofMinutes(5)) // 可选, 默认 5 分钟
        .build();
broker.subscribe();

// 停服时: 对所有持有中的实体做 offline, 等落库完成
data.close();
```

`build()` 会在 broker 上注册数据层的消息，要在 `broker.subscribe()` 之前调用，并且同一个频道上的每台服务器都在同样的位置调用。存储、线程池和 broker 归宿主所有，由宿主在 `data.close()` 之后关闭。

注册实体类型时建表：`{前缀}{类型}_data` 和 `{前缀}{类型}_ranked`。前缀最多 24 个字符，只能用 `[a-z0-9_]`。

## 实体类型和键

```java
EntityType<UUID> player = data.registerEntityType("player", EntityIdCodec.UUID);

// 内存键: register() 要等所有持有中的实体都读完这个新键才完成
MemoryKey<UUID, Integer> mana = player.memory(Key.sparrow("mana"), ValueCodecs.INT, 100).register().join();
MemoryKey<UUID, Optional<String>> title = player.memory(Key.sparrow("title"), ValueCodecs.STRING).register().join();
SortableMemoryKey<UUID> level = player.memoryLong(Key.sparrow("level")).sortable().register().join();
MemoryMapKey<UUID, String, Home> homes = player.memoryMap(Key.sparrow("homes"), EntryKeyCodec.STRING, HOME).register().join();

// DB 键: register() 返回时就能用
DbKey<UUID, Profile> profile = player.db(Key.sparrow("profile"), PROFILE, Profile.EMPTY).register();
SortableDbCounterKey<UUID> coins = player.dbCounter(Key.sparrow("coins")).scale(2).sortable().register();
DbMapKey<UUID, String, Mail> mailbox = player.dbMap(Key.sparrow("mailbox"), EntryKeyCodec.STRING, MAIL).register();
DbMapCounterKey<UUID, String> tokens = player.dbMapCounter(Key.sparrow("tokens"), EntryKeyCodec.STRING).register();
```

- 类型名是 1 到 32 个 `[a-z0-9_]` 字符。键 ID 最多 64 个字符，不能含 `.`。实体 ID 编码后最多 64 字节。
- 注册时不给默认值，值类型就是 `Optional<T>`；写入 `Optional.empty()` 就是删掉存储的值。
- `memoryLong` 键和 DB 计数键可以声明 `sortable()`，之后能用 `top` 和 `rankOf`。映射计数键不能排行。
- 键可以在运行中注册和注销。`unregister` 保留数据；`purge(Key)` 跨所有实体删掉一个键的数据，只能在没有任何服务器还注册着这个键时调用。
- `entity.delete()` 删掉一个实体的全部数据，调用时不能有服务器持有它。

## 内存权威

由宿主的驻留逻辑决定哪台服务器持有实体，并调用 `online` / `offline`。同一时刻只能有一台服务器持有一个实体。

```java
player.residence().online(uuid);          // 读出内存键, 开始定时落库

Entity<UUID> entity = player.entity(uuid);
entity.write(mana, 80);
entity.update(level, current -> current + 1);
entity.put(homes, "base", home);
int value = entity.get(mana);

entity.publish(mana);                     // 把当前值推进其他服的快照, 不落库
entity.save();                            // 立刻落库

player.residence().offline(uuid);         // 先释放, 再把释放之前写的落库
```

- 内存键只能在持有者上读写，在其他地方调用会抛 `IllegalStateException`。同一个实体的所有修改和落库由一把实体级的锁串行执行。
- 读出来的值不能原地修改，要改就在 `update` / `compute` 里返回新值。
- 其他服务器用 `peek` 读快照（还没加载时返回 `null`，第一次调用会发起加载），或者用 `read` 拿一个直接读存储的 future。
- `override(key, value)` / `reset(key)` 在任何服务器上都能调用，不管实体有没有被持有：新值写进存储，再推给持有者。
- 服务器崩溃时没落库的内存数据会丢，丢多少取决于落库间隔。

## DB 权威

```java
entity.add(coins, coins.toUnits(12.5, RoundingMode.UNNECESSARY));
entity.take(coins, price).thenAccept(balance -> {
    if (balance.isEmpty()) {
        // 余额不足, 什么都没写
    }
});
entity.update(profile, current -> current.withMotto("hello"));
entity.putIfAbsent(mailbox, mailId, mail);

data.transaction(tx -> {
    if (tx.take(from, coins, amount).isEmpty()) return false; // 返回 false 就回滚
    tx.add(to, coins, amount);
    return true;
});

player.top(coins, 0, 10);
player.rankOf(coins, uuid);
```

- `update` 是以存储里的字节为条件的 CAS 循环，operator 可能执行多次，不能有副作用。新值编码后和原来字节相同时不写库，也不广播。
- 遇到死锁时事务体会重跑。事务体抛出的任何异常都会让事务回滚，包括 `Error`。失效消息在提交之后才广播。
- 计数键里不存在的值或条目当作 0。余额不够时 `take` 失败，什么都不写。
- `top` 和 `rankOf` 读的是排行表。内存键落库时才写进排行表，所以排行最多落后一个落库间隔。

## 观察标量值

先订阅，再用 `peek` 读首值。回调只提示重新读取，连续变化可以合并；不能用回调次数统计写入次数。

```java
Watch watch = target.watch(likesKey, () -> refresh(target.peek(likesKey)));
refresh(target.peek(likesKey));
```

未加载时 `peek` 返回 null；首读得到 0 也会通知。已有值不补发首值。活动订阅共享本服快照和一次在途读取。用完调用 `watch.close()`，最后一个订阅关闭后快照恢复正常过期。回调在发布变化的线程执行，可能并发；回调只做快速、线程安全的工作。关闭不等待已经开始的回调。

内存标量跟随 `peek`：本服持有时，本地写入或覆盖立即可见，其他服仍通过 publish 或落库后的快照跟进。取得或释放持有权时，仅在可读值改变时通知。释放先于最终落库，因此可能暂时看到旧快照或 null，再收到最终值。DB 标量不切换读源。

## 值的编解码

键的值由 `ValueCodec<T>` 编成字节：

```java
// 内置: ValueCodecs.BOOLEAN, INT, LONG, DOUBLE, STRING, UUID, BYTES
static final ValueCodec<Rank> RANK = ValueCodecs.STRING.xmap(Rank::valueOf, Rank::name);
static final ValueCodec<List<UUID>> MEMBERS = ValueCodec.list(ValueCodecs.UUID);
static final ValueCodec<Map<String, Integer>> KILLS = ValueCodec.map(ValueCodecs.STRING, ValueCodecs.INT);
static final ValueCodec<Optional<UUID>> LEADER = ValueCodec.optional(ValueCodecs.UUID);
static final ValueCodec<Snapshot> SNAPSHOT = ValueCodec.of(Snapshot::toBytes, Snapshot::fromBytes);

record Home(String world, double x, double y, double z, String server) {}
static final ValueCodec<Home> HOME = ValueCodec.struct(
        (home, out) -> {
            out.write(ValueCodecs.STRING, home.world());
            out.write(ValueCodecs.DOUBLE, home.x());
            out.write(ValueCodecs.DOUBLE, home.y());
            out.write(ValueCodecs.DOUBLE, home.z());
            out.write(ValueCodecs.STRING, home.server());   // 第二版加的字段
        },
        in -> new Home(
                in.read(ValueCodecs.STRING),
                in.read(ValueCodecs.DOUBLE),
                in.read(ValueCodecs.DOUBLE),
                in.read(ValueCodecs.DOUBLE),
                in.hasRemaining() ? in.read(ValueCodecs.STRING) : "lobby"   // 第一版的数据没有 server
        )
);
```

- 同一个值每次都要编出同样的字节，`decode(encode(v))` 要等于 `v`。编码不稳定时结果仍然正确，只是值不变的 update 也会写库。map 按迭代顺序编码，在意这一点就用 `LinkedHashMap` 或 `TreeMap`。
- `decode` 可能在 Redis 的订阅线程上执行，所以要快，并且线程安全。返回的值不能引用传进来的数组。
- 解码出来的 list 和 map 不可修改。struct 的字段带长度前缀、按顺序读；新字段加在末尾，读的一方会忽略自己不认识的尾部数据。
- 组合出来的 codec 存成常量，`list`、`map`、`optional`、`struct` 每次调用都会新建一个 codec。

## 观察值的变化

`watch` 在一个键（或映射的一个条目）能看到的值可能变了时通知：本服持有这个实体时是持有容器里的值，其他情况是快照里的值。回调不带值，用 `peek` 读。

```java
// 打开离线玩家的资料菜单, 不显示加载中, 之后跟着变
Watch watch = target.watch(PROFILE, menu::refresh);
watch.ready()
        .thenRunAsync(() -> menu.open(target.peek(PROFILE)), mainThread)
        .exceptionally(error -> {
            watch.close();
            return null;
        });
// 菜单关闭时
watch.close();
```

- `ready()` 在 `peek` 能读到值时完成：本服持有或快照已经加载过时立刻完成，否则在首次加载完成时完成，并且先于第一次回调。首次加载失败时带着存储的异常失败；在此之前 watch 被关闭、键被注销或数据层关闭时被取消。
- 回调和 `ready()` 后面的步骤在触发改动的线程上执行，可能是写入线程、Executor 或 Redis 的订阅线程。回调要快，其他事情切到自己的线程去做。
- 有人观察的快照不会过期，打开的 watch 都要关掉。

## 小数计数

`dbCounter`、`dbMapCounter`、`memoryLong` 都可以设 `scale(int)`（0 到 18）。键存取的仍然是最小单位的 `long`，精度只用来换算：

```java
long units = coins.toUnits(new BigDecimal("19.99"), RoundingMode.UNNECESSARY); // 1999
long fromVault = coins.toUnits(amount, RoundingMode.DOWN);                     // 输入是 double
BigDecimal shown = coins.toDecimal(units);                                    // 19.99, 小数位数总是 scale
double forVault = coins.toDouble(units);
```

`toUnits(double, ...)` 按 double 打印出来的十进制数换算，`0.57` 得到 57。换算之前在 double 里做的运算，误差还是会带进来。已经有数据的键不能改精度，否则库里的值读出来会差 10 的 n 次方倍。

## 限制

- 假定 Redis 和数据库始终可用，丢失的消息和基础设施故障不做补偿。
- 覆盖消息如果正好赶上持有者 `offline` 时的最终落库，这次覆盖可能会丢：最终落库把旧值写回了库，已经释放的持有者又丢掉了这条消息。
- 条目值里嵌套的 map 整体存成一个值；映射计数键不能排行。