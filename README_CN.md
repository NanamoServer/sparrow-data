# Sparrow Data

[English](./README.md) · **简体中文**

给 Minecraft 群组服插件用的数据层。数据挂在实体上（玩家、公会，任何有 ID 的东西），用带类型的键读写，由它负责加载、落库和跨服同步。

| 键 | 数据在哪 | 适合 |
|---|---|---|
| 内存键 | 管着实体的那台服的内存里，定时落库 | 只有一台服在改、读写频繁的数据，如魔力值 |
| DB 键 | 数据库，每次修改直接写 | 多台服都可能改的数据，如金币 |
| 视图键 | 你自己的表 | 只想要缓存和跨服刷新，如家 |
| 名单键 | Redis | 全网现在谁处于某个状态，如隐身 |

## 引入

```kotlin
repositories {
    maven("https://repo.momirealms.net/snapshots")
}

dependencies {
    implementation("net.momirealms:sparrow-data:0.3")
}
```

需要 Java 21。以下依赖是 `compileOnly`，由宿主提供：

| 依赖 | 使用的版本 |
|---|---|
| `net.momirealms:sparrow-message` | 26.11.1 |
| `io.lettuce:lettuce-core` | 7.7.0.RELEASE |
| `io.netty:netty-buffer` | 4.2.7.Final |
| `com.github.ben-manes.caffeine:caffeine` | 3.2.3 |
| `org.slf4j:slf4j-api` | 2.0.17 |

## 生命周期

```java
// 1. 创建. build() 会在 broker 上注册消息, 必须在 broker.subscribe() 之前
DataService data = SparrowDataService.builder()
        .storage(new MysqlEntityStorage(dataSource, "sparrow_"))   // 或 MariaDbEntityStorage / PostgresEntityStorage
        .executor(executor)                     // 数据库调用都在这个线程池上执行
        .messageBroker(broker)
        .saveInterval(Duration.ofMinutes(5))    // 内存键自动落库的间隔
        .snapshotExpiry(Duration.ofMinutes(5))  // 可选, 快照多久没人读就丢掉, 默认 5 分钟
        .redis(redisClient)                     // 可选, 只有名单键需要
        .serverAlive(servers::isOnline)         // 可选, 只有名单键需要
        .build();

// 2. 订阅
broker.subscribe();

// 3. 注册实体类型和键, 运行中也可以随时注册和注销
EntityType<UUID> player = data.registerEntityType("player", EntityIdCodec.UUID);

// 4. 关服. 让所有在线实体下线并等落库完成, 之后再关你自己的连接池, 线程池和 broker
data.close();
```

## 实体类型

```java
EntityType<UUID> player = data.registerEntityType("player", EntityIdCodec.UUID);   // 还有 STRING, INT, LONG
EntityType<String> guild = data.registerEntityType("guild", EntityIdCodec.STRING);

Entity<UUID> entity = player.entity(uuid);   // 只是一个地址, 随时可以取, 不需要先加载
```

- 类型名是 1 到 32 个 `[a-z0-9_]` 字符，实体 ID 编码后最多 64 字节。
- 每个类型有两张表 `{前缀}{类型}_data` 和 `{前缀}{类型}_ranked`，第一次注册内存键或 DB 键时创建。前缀最多 24 个 `[a-z0-9_]` 字符。

## 内存键

值在管着实体的那台服的内存里，读写不碰数据库。

```java
// 注册. 返回 future, 运行中注册时要等已在线的实体补读完这个键
MemoryKey<UUID, Integer> MANA = player.memory(Key.sparrow("mana"), ValueCodecs.INT, 100).register().join();          // 默认值 100
MemoryKey<UUID, Optional<String>> TITLE = player.memory(Key.sparrow("title"), ValueCodecs.STRING).register().join();  // 不给默认值, 类型是 Optional
SortableMemoryKey<UUID> LEVEL = player.memoryLong(Key.sparrow("level")).sortable().register().join();                 // long, 可排行

// 上线: 从数据库读出全部内存键, 开始定时落库
player.residence().online(uuid).join();

Entity<UUID> entity = player.entity(uuid);
int mana = entity.get(MANA);
entity.write(MANA, mana - 20);
entity.update(LEVEL, level -> level + 1);        // 读改写一步完成
entity.write(TITLE, Optional.empty());           // 可选键写空就是删除

entity.save();                                   // 立刻落库, 不等定时
entity.publish(MANA);                            // 把当前值推给其他服的快照, 不落库

// 下线: 落库, 然后从内存移除
player.residence().offline(uuid).join();
```

- **同一时刻只能有一台服对同一个实体 `online`。** 换服时新服要等旧服的 `offline` 完成，这把锁由宿主负责。
- `get` / `write` / `update` / `save` / `publish` 只能在 `online` 的那台服上调用，否则抛 `IllegalStateException`。
- 读出的对象不要原地修改，用 `update` 返回新对象。
- 崩溃时丢失上次落库之后的修改。

任何一台服都可以改别的服上的、或者离线的实体：

```java
player.entity(target).override(TITLE, Optional.of("管理员"));   // 写库, 再通知管着他的那台服换成新值
player.entity(target).reset(MANA);                              // 恢复默认值
```

## DB 键

每次修改都是一条数据库语句，任何服随时可以写，不需要 `online`。

```java
// 注册, 立即可用
DbKey<UUID, GuildInfo> INFO = guild.db(Key.sparrow("info"), GUILD_INFO, GuildInfo.EMPTY).register();
DbCounterKey<UUID> GEMS = player.dbCounter(Key.sparrow("gems")).register();
SortableDbCounterKey<UUID> COINS = player.dbCounter(Key.sparrow("coins")).sortable().register();

Entity<UUID> entity = player.entity(uuid);

// 计数键. 没有值时当作 0
entity.add(COINS, 100);                              // CompletableFuture<Long>, 新余额
entity.take(COINS, 30).thenAccept(balance -> {       // CompletableFuture<OptionalLong>
    if (balance.isEmpty()) return;                   // 余额不足, 一分没扣
});
entity.set(COINS, 0);

// 普通 DB 键
guild.entity(guildId).set(INFO, info);
guild.entity(guildId).update(INFO, old -> old.withNotice("周六打本"));   // 以旧值为条件写入, 冲突就重读重算
```

`update` 的函数可能执行多次，里面只做计算，不要发消息或给物品。

### 事务

```java
CompletableFuture<String> result = data.transaction(tx -> {
    if (tx.take(from, COINS, amount).isEmpty()) {
        tx.rollback();               // 撤销前面的修改, 返回值照样交给调用方
        return "余额不足";
    }
    tx.add(to, COINS, amount);
    return "转账成功";                // 正常返回即提交, 抛异常则回滚
});
```

死锁时事务体会自动重跑，所以它同样可能执行多次。

### 排行

```java
player.top(COINS, 0, 10);        // CompletableFuture<List<RankEntry<UUID>>>, 从第 1 名起取 10 个
player.rankOf(COINS, uuid);      // CompletableFuture<Long>, 名次从 1 开始, 数值相同名次相同
```

只有 `sortable()` 的键能排行。内存键落库后才进排行，最多落后一个 `saveInterval`。

## 在其他服上读

对内存键、DB 键和视图键都一样。

```java
Entity<UUID> target = player.entity(targetId);

target.read(COINS);              // CompletableFuture<T>, 读最新的值
Long coins = target.peek(COINS); // 同步读本服的快照. 还没加载时返回 null 并在后台开始加载
```

快照在数据变化时自动刷新，`snapshotExpiry` 内没人读就丢掉。

### watch 和 pin

```java
// watch: 值变了就回调. 回调不带值, 用 peek 读
Watch watch = target.watch(COINS, () -> menu.refresh(target.peek(COINS)));
watch.ready().thenRun(() -> menu.open(target.peek(COINS)));   // ready 在 peek 能读到值时完成
watch.close();                                                // 用完必须关

// pin: 只让快照保持加载, 不回调. 玩家在线期间需要同步读时用
Pin pin = target.pin(COINS);
pin.ready().join();
long balance = target.peek(COINS);                            // close 之前不会是 null
pin.close();
```

- 被 watch 或 pin 的快照不过期。
- 值有没有变按 `equals` 判断，自定义的值类型要实现 `equals`，用 record 即可。
- 回调可能在写入线程、Executor 或 Redis 线程上执行，要快，操作 Bukkit 前切回主线程。连续变化可能合并成一次通知。

## 视图键

数据在你自己的表里，Sparrow Data 只负责快照和跨服刷新。

```java
// 注册. loader 是你自己的查询, 返回 CompletableFuture, 结果不能是 null
ViewKey<UUID, List<Home>> HOMES = player.view(Key.sparrow("homes"), owner -> homeStore.load(owner)).register();

Entity<UUID> entity = player.entity(owner);
entity.peek(HOMES);                       // peek / read / watch / pin 和其他键一样用
entity.read(HOMES);                       // 每次都调用 loader

// 改完自己的表之后通知一声: 本服重读快照, 其他服也重读
homeStore.create(home).thenCompose(done -> entity.invalidate(HOMES));   // future 完成时本服 peek 已是新数据
```

加载出来的对象被所有读取方共用，不要修改。只有视图键的实体类型不建表。

## 名单键

一张全网共享的表：实体 ID 对应一个值。每台服在内存里有完整的镜像，查询是同步的。用来回答“全网现在谁在隐身 / 挂机 / 值班”。数据在 Redis 里，不落库。

```java
// 注册. 必须在 broker.subscribe() 之后, future 在镜像装好后完成
RosterKey<UUID, String> VANISHED = player.roster(Key.sparrow("vanished"), ValueCodecs.STRING).register().join();

Entity<UUID> entity = player.entity(uuid);

// 写入. 记录归本服所有, 返回前本服就能查到
entity.announce(VANISHED, playerName);

// 任何一台服上同步查询
String name = entity.member(VANISHED);                 // 不在名单里时为 null
Map<UUID, String> all = player.members(VANISHED);      // 当前全部成员的副本

// 变化通知
Watch whole = player.watch(VANISHED, menu::refresh);   // 任何成员进出或值变了
Watch single = entity.watch(VANISHED, tag::refresh);   // 只看这一个实体

// 撤回. 只删归本服所有的记录
entity.withdraw(VANISHED);
```

记录的归属和清理：

```java
// 生存服
entity.announce(VANISHED, name);    // 记录归生存服
// 玩家切到资源服, 资源服
entity.announce(VANISHED, name);    // 覆盖, 记录改归资源服
// 生存服此时才处理完退出
entity.withdraw(VANISHED);          // 什么都不删, 记录已经不归生存服
// 资源服
entity.withdraw(VANISHED);          // 删除, 所有服都查不到了
```

| 情况 | 发生什么 |
|---|---|
| 注册名单键 | 先清掉本服上次运行留下的记录，再装载镜像 |
| 注销键、`data.close()` | 清掉本服名下的全部记录 |
| 实体 `offline` | 不会自动撤回，要自己调用 `withdraw` |
| 服务器崩溃 | 记录留到它重启；传了 `serverAlive` 时，其他服查询会跳过不存活的服的记录 |
| Redis 断线或被清空 | 重连后每台服自动补回自己的记录并重新装载镜像 |

`redis(...)` 传入的 `RedisClient` 必须和 broker 用的是同一个。

## 值的编解码

```java
// 内置: ValueCodecs.BOOLEAN, INT, LONG, DOUBLE, STRING, UUID, BYTES
static final ValueCodec<Rank> RANK = ValueCodecs.STRING.xmap(Rank::valueOf, Rank::name);
static final ValueCodec<List<UUID>> MEMBERS = ValueCodec.list(ValueCodecs.UUID);
static final ValueCodec<Map<String, Integer>> KILLS = ValueCodec.map(ValueCodecs.STRING, ValueCodecs.INT);
static final ValueCodec<ItemStack> ITEM = ValueCodec.of(ItemStack::serializeAsBytes, ItemStack::deserializeBytes);

// record 用 struct, 按顺序写, 按顺序读
record Home(String world, double x, double y, double z, String server) {}
static final ValueCodec<Home> HOME = ValueCodec.struct(
        (home, out) -> {
            out.write(ValueCodecs.STRING, home.world());
            out.write(ValueCodecs.DOUBLE, home.x());
            out.write(ValueCodecs.DOUBLE, home.y());
            out.write(ValueCodecs.DOUBLE, home.z());
            out.write(ValueCodecs.STRING, home.server());   // 新字段只能加在末尾
        },
        in -> new Home(
                in.read(ValueCodecs.STRING),
                in.read(ValueCodecs.DOUBLE),
                in.read(ValueCodecs.DOUBLE),
                in.read(ValueCodecs.DOUBLE),
                in.hasRemaining() ? in.read(ValueCodecs.STRING) : "lobby"   // 旧数据里没有这个字段
        )
);
```

- 组合出来的 codec 存成常量，`list` / `map` / `struct` 每次调用都新建一个。
- 同样的值每次要编出同样的字节，`map` 里用 `LinkedHashMap` 或 `TreeMap`。
- 解码出来的 list 和 map 只读。
- 值的类型要实现 `equals`。

## 小数计数

计数键存的永远是 `long`。`scale(n)` 只负责和小数互相换算，范围 0 到 18。

```java
SortableDbCounterKey<UUID> COINS = player.dbCounter(Key.sparrow("coins")).scale(2).sortable().register();

long units = COINS.toUnits(new BigDecimal("19.99"), RoundingMode.UNNECESSARY);   // 1999
long fromDouble = COINS.toUnits(12.5, RoundingMode.DOWN);                        // 1250
BigDecimal shown = COINS.toDecimal(1999);                                        // 19.99
double asDouble = COINS.toDouble(1999);
```

有了数据的键不能再改 `scale`。`memoryLong` 同样支持。

## 注销和删除

```java
player.unregister(MANA);                 // 注销键, 数据留在库里
player.purge(Key.sparrow("mana"));       // 删掉所有实体的这项数据, 要先在每台服上注销
player.entity(uuid).delete();            // 删掉一个实体的全部数据, 这时不能有服务器 online 着它
```

键 ID 最多 64 个字符，不能包含 `.`。

## 限制

- 假定数据库和 Redis 始终可用。除名单键会在重连后自动修复外，丢失的跨服消息不补发：其他服的快照要等过期重载才更新，被 watch 或 pin 的快照要等下一次修改。
- `override` 正好撞上实体 `offline` 时的最后一次落库时，可能被旧值盖掉。
- 放进一个值里的集合整体存成一行，改一项也要重写整个值。