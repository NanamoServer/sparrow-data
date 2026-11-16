# Sparrow Data

**English** · [简体中文](./README_CN.md)

A data layer for Minecraft network plugins. Data is attached to entities (players, guilds, anything with an id) and read and written through typed keys. Loading, saving and cross-server sync are handled for you.

| Key | Where the data lives | Fits |
|---|---|---|
| Memory key | In the memory of the server in charge of the entity, saved on an interval | data only one server changes and that is read and written often, such as mana |
| DB key | The database, every change is written at once | data several servers may change, such as coins |
| View key | Your own table | when you only want caching and cross-server refresh, such as homes |
| Roster key | Redis | who across the network is in some state right now, such as vanished |

## Installation

```kotlin
repositories {
    maven("https://repo.momirealms.net/snapshots")
}

dependencies {
    implementation("net.momirealms:sparrow-data:0.0.4")
}
```

Requires Java 21. These dependencies are `compileOnly` and must be provided by the host:

| Dependency | Version used |
|---|---|
| `net.momirealms:sparrow-message` | 26.11.1 |
| `io.lettuce:lettuce-core` | 7.7.0.RELEASE |
| `io.netty:netty-buffer` | 4.2.7.Final |
| `com.github.ben-manes.caffeine:caffeine` | 3.2.3 |
| `org.slf4j:slf4j-api` | 2.0.17 |

## Lifecycle

```java
// 1. Create. build() registers messages on the broker, so it must come before broker.subscribe()
DataService data = SparrowDataService.builder()
        .storage(new MysqlEntityStorage(dataSource, "sparrow_"))   // or MariaDbEntityStorage / PostgresEntityStorage
        .executor(executor)                     // every database call runs on this pool
        .messageBroker(broker)
        .saveInterval(Duration.ofMinutes(5))    // how often memory keys are saved automatically
        .snapshotExpiry(Duration.ofMinutes(5))  // optional, how long an unread snapshot is kept, default 5 minutes
        .redis(redisClient)                     // optional, only roster keys need it
        .serverAlive(servers::isOnline)         // optional, only roster keys need it
        .build();

// 2. Subscribe
broker.subscribe();

// 3. Register entity types and keys. They can also be registered and unregistered while running
EntityType<UUID> player = data.registerEntityType("player", EntityIdCodec.UUID);

// 4. Shutdown. Takes every online entity offline and waits for the saves, then close your own pool, executor and broker
data.close();
```

## Entity types

```java
EntityType<UUID> player = data.registerEntityType("player", EntityIdCodec.UUID);   // also STRING, INT, LONG
EntityType<String> guild = data.registerEntityType("guild", EntityIdCodec.STRING);

Entity<UUID> entity = player.entity(uuid);   // only an address, available at any time, nothing to load first
```

- Type names are 1 to 32 characters of `[a-z0-9_]`. An encoded entity id is at most 64 bytes.
- Each type has two tables, `{prefix}{type}_data` and `{prefix}{type}_ranked`, created when its first memory key or DB key is registered. The prefix is at most 24 characters of `[a-z0-9_]`.

## Memory keys

The value sits in the memory of the server in charge of the entity. Reads and writes never touch the database.

```java
// Register. Returns a future: when registered at runtime, entities already online have to load the new key first
MemoryKey<UUID, Integer> MANA = player.memory(Key.sparrow("mana"), ValueCodecs.INT, 100).register().join();          // default 100
MemoryKey<UUID, @Nullable String> TITLE = player.memory(Key.sparrow("title"), ValueCodecs.STRING).register().join();  // no default, reads null until it is set
SortableMemoryKey<UUID> LEVEL = player.memoryLong(Key.sparrow("level")).sortable().register().join();                 // a long that can be ranked

// Online: read every memory key from the database and start the save interval
player.residence().online(uuid).join();

Entity<UUID> entity = player.entity(uuid);
int mana = entity.get(MANA);
entity.write(MANA, mana - 20);
entity.update(LEVEL, level -> level + 1);        // read, change and write in one step
entity.write(TITLE, null);                       // writing null deletes the stored value

entity.save();                                   // save now instead of waiting for the interval
entity.publish(MANA);                            // push the current value into other servers' snapshots, no save

// Offline: save, then drop from memory
player.residence().offline(uuid).join();
```

- **Only one server may have an entity `online` at any moment.** On a server switch the new server must wait for the old server's `offline` to finish. That lock is the host's job.
- `get` / `write` / `update` / `save` / `publish` only work on the server that called `online`. Elsewhere they throw `IllegalStateException`.
- Do not modify an object you read. Return a new one from `update`.
- Writing null deletes the stored value. This holds for `write`, the value returned from `update`, `override` and `set` on DB keys. Afterwards a key with a default reads the default and a key without one reads null.
- A crash loses the changes made after the last save.

Any server can change an entity that is on another server or offline:

```java
player.entity(target).override(TITLE, "Admin");   // writes the database, then tells the server in charge to switch
player.entity(target).override(MANA, null);       // deletes the stored value, reads give the default
```

## DB keys

Every change is one database statement. Any server can write at any time, with no `online` needed.

```java
// Register, usable at once
DbKey<UUID, GuildInfo> INFO = guild.db(Key.sparrow("info"), GUILD_INFO, GuildInfo.EMPTY).register();
DbCounterKey<UUID> GEMS = player.dbCounter(Key.sparrow("gems")).register();
SortableDbCounterKey<UUID> COINS = player.dbCounter(Key.sparrow("coins")).sortable().register();

Entity<UUID> entity = player.entity(uuid);

// Counter keys. A missing value counts as 0
entity.add(COINS, 100);                              // CompletableFuture<Long>, the new balance
entity.take(COINS, 30).thenAccept(balance -> {       // CompletableFuture<OptionalLong>
    if (balance.isEmpty()) return;                   // not enough, nothing was taken
});
entity.set(COINS, 0);

// Plain DB keys
guild.entity(guildId).set(INFO, info);
guild.entity(guildId).update(INFO, old -> old.withNotice("Raid on Saturday"));   // writes on condition of the old value, re-reads and recomputes on conflict
```

The function passed to `update` may run several times. Keep it to computation, with no messages sent or items given inside.

### Transactions

```java
CompletableFuture<String> result = data.transaction(tx -> {
    if (tx.take(from, COINS, amount).isEmpty()) {
        tx.rollback();               // undoes the earlier changes, the return value still reaches the caller
        return "Not enough coins";
    }
    tx.add(to, COINS, amount);
    return "Sent";                   // a normal return commits, an exception rolls back
});
```

The body is run again automatically after a deadlock, so it may run several times too.

### Ranking

```java
player.top(COINS, 0, 10);        // CompletableFuture<List<RankEntry<UUID>>>, 10 entries starting from the first place
player.rankOf(COINS, uuid);      // CompletableFuture<Long>, positions start at 1, equal values share a position
```

Only `sortable()` keys can be ranked. A memory key enters the ranking when it is saved, so it lags by up to one `saveInterval`.

## Reading from other servers

The same for memory keys, DB keys and view keys.

```java
Entity<UUID> target = player.entity(targetId);

target.read(COINS);              // CompletableFuture<T>, reads the latest value
Long coins = target.readCachedOrNull(COINS);        // synchronous read of this server's snapshot. Returns null until cached and starts loading in the background
target.readCachedOrNull(COINS, false);              // reads an existing snapshot only, never starts a load
target.isCached(COINS);                             // whether a read would find it. A key without a default can cache null, this tells the two apart
```

A snapshot refreshes when the data changes and is dropped when nobody reads it for `snapshotExpiry`.

### watch and pin

```java
// watch: calls back when the value changes. The callback carries no value, read it with readCachedOrNull
Watch watch = target.watch(COINS, () -> menu.refresh(target.readCachedOrNull(COINS)));
watch.ready().thenRun(() -> menu.open(target.readCachedOrNull(COINS)));   // ready completes once the snapshot can be read
watch.close();                                                // always close it

// pin: only keeps the snapshot loaded, no callback. For synchronous reads while a player is online
Pin pin = target.pin(COINS);
pin.ready().join();
long balance = target.readCachedOrNull(COINS);                // always readable until close
pin.close();
```

- A watched or pinned snapshot does not expire.
- Whether a value changed is decided with `equals`. Your own value types need `equals`; a record is enough.
- Callbacks may run on the writing thread, the executor or a Redis thread. Keep them fast and switch to the main thread before touching Bukkit. Quick successive changes may arrive as one notification.

## View keys

The data lives in your own table. Sparrow Data only provides snapshots and cross-server refresh.

```java
// Register. The loader is your own query, returns a CompletableFuture and must not yield null
ViewKey<UUID, List<Home>> HOMES = player.view(Key.sparrow("homes"), owner -> homeStore.load(owner)).register();

Entity<UUID> entity = player.entity(owner);
entity.readCachedOrNull(HOMES);           // readCachedOrNull / read / watch / pin work as on any other key
entity.read(HOMES);                       // calls the loader every time

// After changing your own table, say so: this server reloads its snapshot and so do the others
homeStore.create(home).thenCompose(done -> entity.invalidate(HOMES));   // when the future completes, reads here return the new data
```

Loaded objects are shared by every reader, so do not modify them. An entity type that only has view keys gets no tables.

## Roster keys

A table shared by the whole network: entity id to value. Every server keeps a full mirror in memory, so lookups are synchronous. It answers "who across the network is vanished / AFK / on duty right now". The data lives in Redis and is never saved to the database.

```java
// Register. Must come after broker.subscribe(); the future completes once the mirror is loaded
RosterKey<UUID, String> VANISHED = player.roster(Key.sparrow("vanished"), ValueCodecs.STRING).register().join();

Entity<UUID> entity = player.entity(uuid);

// Write. The record belongs to this server and is visible here before the call returns
entity.announce(VANISHED, playerName);

// Synchronous lookup on any server
String name = entity.member(VANISHED);                 // null when not in the roster
Map<UUID, String> all = player.members(VANISHED);      // a copy of all current members

// Change notifications
Watch whole = player.watch(VANISHED, menu::refresh);   // any member enters, leaves or changes value
Watch single = entity.watch(VANISHED, tag::refresh);   // only this entity

// Withdraw. Removes the record only if this server owns it
entity.withdraw(VANISHED);
```

Ownership and cleanup of a record:

```java
// survival
entity.announce(VANISHED, name);    // the record belongs to survival
// the player switches to the resource server, on the resource server
entity.announce(VANISHED, name);    // overwrites, the record now belongs to the resource server
// survival only now finishes handling the quit
entity.withdraw(VANISHED);          // removes nothing, the record no longer belongs to survival
// resource server
entity.withdraw(VANISHED);          // removed, no server sees it any more
```

| Situation | What happens |
|---|---|
| Registering a roster key | records this server left from its previous run are cleared, then the mirror is loaded |
| Unregistering the key, `data.close()` | every record this server owns is cleared |
| An entity goes `offline` | nothing is withdrawn automatically, call `withdraw` yourself |
| A server crashes | its records stay until it restarts; with `serverAlive` set, other servers skip records of a server that is not alive |
| Redis disconnects or is wiped | after reconnecting, every server restores its own records and reloads the mirror |

The `RedisClient` passed to `redis(...)` must be the same one the broker uses.

## Values

```java
// Built in: ValueCodecs.BOOLEAN, INT, LONG, DOUBLE, STRING, UUID, BYTES
static final ValueCodec<Rank> RANK = ValueCodecs.STRING.xmap(Rank::valueOf, Rank::name);
static final ValueCodec<List<UUID>> MEMBERS = ValueCodec.list(ValueCodecs.UUID);
static final ValueCodec<Map<String, Integer>> KILLS = ValueCodec.map(ValueCodecs.STRING, ValueCodecs.INT);
static final ValueCodec<ItemStack> ITEM = ValueCodec.of(ItemStack::serializeAsBytes, ItemStack::deserializeBytes);

// Use struct for records: fields are written in order and read in order
record Home(String world, double x, double y, double z, String server) {}
static final ValueCodec<Home> HOME = ValueCodec.struct(
        (home, out) -> {
            out.write(ValueCodecs.STRING, home.world());
            out.write(ValueCodecs.DOUBLE, home.x());
            out.write(ValueCodecs.DOUBLE, home.y());
            out.write(ValueCodecs.DOUBLE, home.z());
            out.write(ValueCodecs.STRING, home.server());   // new fields go at the end only
        },
        in -> new Home(
                in.read(ValueCodecs.STRING),
                in.read(ValueCodecs.DOUBLE),
                in.read(ValueCodecs.DOUBLE),
                in.read(ValueCodecs.DOUBLE),
                in.hasRemaining() ? in.read(ValueCodecs.STRING) : "lobby"   // old data does not have this field
        )
);
```

- Store composed codecs in constants. `list` / `map` / `struct` build a new one on every call.
- The same value must always encode to the same bytes. Use `LinkedHashMap` or `TreeMap` inside `map`.
- Decoded lists and maps are read-only.
- Value types need `equals`.

## Decimal counters

A counter key always stores a `long`. `scale(n)` only converts to and from decimals, with n from 0 to 18.

```java
SortableDbCounterKey<UUID> COINS = player.dbCounter(Key.sparrow("coins")).scale(2).sortable().register();

long units = COINS.toUnits(new BigDecimal("19.99"), RoundingMode.UNNECESSARY);   // 1999
long fromDouble = COINS.toUnits(12.5, RoundingMode.DOWN);                        // 1250
BigDecimal shown = COINS.toDecimal(1999);                                        // 19.99
double asDouble = COINS.toDouble(1999);
```

Never change the `scale` of a key that already has data. `memoryLong` supports it as well.

## Unregistering and deleting

```java
player.unregister(MANA);                 // unregister a key, its data stays in the database
player.purge(Key.sparrow("mana"));       // delete this data for every entity, unregister it on every server first
player.entity(uuid).delete();            // delete everything of one entity, no server may have it online
```

Key ids are at most 64 characters and must not contain `.`.

## No map keys

A key holds exactly one value.

| Situation | What to do |
|---|---|
| The entries are fixed or come from config | register one key per entry, such as one counter key per currency |
| The entries are not fixed, but there are few | put the whole collection into one value, such as `MemoryKey<UUID, List<Home>>` |
| There are many, or they need queries or constraints | your own table with a view key |

## Limitations

- The database and Redis are assumed to be always available. Roster keys repair themselves after a reconnect. Everywhere else a lost cross-server message is not sent again: other servers' snapshots update only when they expire and reload, and a watched or pinned snapshot only on the next change.
- An `override` that lands exactly during an entity's final save on going `offline` can be overwritten by the old value.
- A collection kept inside one value is stored as a single row, and changing one item rewrites the whole value.