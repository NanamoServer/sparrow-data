# Sparrow Data

**English** · [简体中文](./README_CN.md)

A per-entity data layer for Minecraft server networks. Data is attached to entities (players, guilds, anything with an id) through typed keys, and every key picks one of two authority models:

- **Memory keys** live on the one server that currently holds the entity. Reads and writes are plain in-memory calls; the data is saved on an interval and on `offline`.
- **DB keys** are authoritative in the database. Every write goes straight to storage as an atomic statement or a CAS loop, so any server can write at any time.

Other servers see both kinds through snapshots that are invalidated over Redis. Storage ships for MySQL, MariaDB and PostgreSQL; anything else can implement `EntityStorage`.

## Installation

```kotlin
repositories {
    maven("https://repo.momirealms.net/snapshots")
}

dependencies {
    implementation("net.momirealms:sparrow-data:0.1")
}
```

Requires Java 21. The following are `compileOnly` and must be provided by the host:

| Dependency | Version used |
|---|---|
| `net.momirealms:sparrow-redis-message-broker` | 0.0.7 |
| `io.netty:netty-buffer` | 4.2.7.Final |
| `com.github.ben-manes.caffeine:caffeine` | 3.2.3 |
| `org.slf4j:slf4j-api` | 2.0.17 |

## Setup

```java
EntityStorage storage = new MysqlEntityStorage(dataSource, "sparrow_"); // MariaDbEntityStorage / PostgresEntityStorage

DataService data = SparrowDataService.builder()
        .storage(storage)
        .executor(executor)                    // runs every blocking storage call
        .messageBroker(broker)
        .saveInterval(Duration.ofMinutes(5))   // auto save of held entities
        .snapshotExpiry(Duration.ofMinutes(5)) // optional, default 5 minutes
        .build();
broker.subscribe();

// on shutdown: offlines every held entity and waits for the saves
data.close();
```

`build()` registers the data layer's messages on the broker. Call it before `broker.subscribe()`, at the same point on every server of the channel. The storage, executor and broker belong to the host, and the host closes them after `data.close()`.

Tables are created when an entity type is registered: `{prefix}{type}_data` and `{prefix}{type}_ranked`. The prefix is at most 24 characters of `[a-z0-9_]`.

## Entity types and keys

```java
EntityType<UUID> player = data.registerEntityType("player", EntityIdCodec.UUID);

// memory keys: register() completes after every held entity has loaded the new key
MemoryKey<UUID, Integer> mana = player.memory(Key.sparrow("mana"), ValueCodecs.INT, 100).register().join();
MemoryKey<UUID, Optional<String>> title = player.memory(Key.sparrow("title"), ValueCodecs.STRING).register().join();
SortableMemoryKey<UUID> level = player.memoryLong(Key.sparrow("level")).sortable().register().join();
MemoryMapKey<UUID, String, Home> homes = player.memoryMap(Key.sparrow("homes"), EntryKeyCodec.STRING, HOME).register().join();

// DB keys: usable as soon as register() returns
DbKey<UUID, Profile> profile = player.db(Key.sparrow("profile"), PROFILE, Profile.EMPTY).register();
SortableDbCounterKey<UUID> coins = player.dbCounter(Key.sparrow("coins")).scale(2).sortable().register();
DbMapKey<UUID, String, Mail> mailbox = player.dbMap(Key.sparrow("mailbox"), EntryKeyCodec.STRING, MAIL).register();
DbMapCounterKey<UUID, String> tokens = player.dbMapCounter(Key.sparrow("tokens"), EntryKeyCodec.STRING).register();
```

- Type names are 1 to 32 characters of `[a-z0-9_]`. Key ids are at most 64 characters and may not contain `.`. An encoded entity id is at most 64 bytes.
- Registering without a default value makes the value type `Optional<T>`; writing `Optional.empty()` deletes the stored value.
- `memoryLong` keys and DB counter keys can be `sortable()`, which enables `top` and `rankOf`. Map counter keys can't be ranked.
- Keys can be registered and unregistered at runtime. `unregister` keeps the data; `purge(Key)` deletes a key's data across all entities and must only be called once no server has the key registered.
- `entity.delete()` removes all data of one entity. No server may hold it at that moment.

## Memory authority

The host's residence logic decides which server holds an entity and calls `online` / `offline`. Exactly one server may hold an entity at a time.

```java
player.residence().online(uuid);          // load memory keys, start auto save

Entity<UUID> entity = player.entity(uuid);
entity.write(mana, 80);
entity.update(level, current -> current + 1);
entity.put(homes, "base", home);
int value = entity.get(mana);

entity.publish(mana);                     // push the current value into other servers' snapshots, no save
entity.save();                            // save now

player.residence().offline(uuid);         // release, then save what was written before
```

- Memory reads and writes only work on the holder; anywhere else they throw `IllegalStateException`. All modifications and saves of one entity are serialized by a per-entity lock.
- Values you get back must not be mutated in place. Return a new value from `update` / `compute` instead.
- Other servers use `peek` (snapshot; `null` until loaded, the first call starts the load) or `read` (a future that reads storage).
- `override(key, value)` / `reset(key)` work from any server, whether or not anyone holds the entity: the value is written to storage and pushed to the holder.
- Unsaved memory data is lost if the server crashes; the save interval bounds the loss.

## DB authority

```java
entity.add(coins, coins.toUnits(12.5, RoundingMode.UNNECESSARY));
entity.take(coins, price).thenAccept(balance -> {
    if (balance.isEmpty()) {
        // insufficient balance, nothing was written
    }
});
entity.update(profile, current -> current.withMotto("hello"));
entity.putIfAbsent(mailbox, mailId, mail);

data.transaction(tx -> {
    if (tx.take(from, coins, amount).isEmpty()) return false; // false rolls back
    tx.add(to, coins, amount);
    return true;
});

player.top(coins, 0, 10);
player.rankOf(coins, uuid);
```

- `update` is a CAS loop on the stored bytes, so its operator may run several times and must be free of side effects. An update that encodes to the same bytes neither writes nor broadcasts.
- A transaction body may run again after a deadlock. Anything it throws, `Error` included, rolls the transaction back. Invalidations are broadcast only after commit.
- Counters treat a missing value or entry as 0. `take` fails without writing when the balance is too low.
- `top` and `rankOf` read the ranking table. Memory keys reach it when they are saved, so their ranking lags by up to one save interval.

## Watching scalar values

Subscribe before reading the initial snapshot:

```java
Watch watch = target.watch(likesKey, () -> refresh(target.peek(likesKey)));
refresh(target.peek(likesKey));
```

An already available value is not replayed. A cold read returns null; its completion notifies even for a zero count. Watches share the local snapshot and its in-flight load. Callbacks can run concurrently on the publishing threads: keep them fast and pull the latest state. Notifications may coalesce. Call `watch.close()` to unsubscribe; this does not wait for callbacks already running. After the last watch closes, ordinary snapshot expiry resumes.

For memory keys the watch follows the same source as `peek`. Local writes and overrides are visible while held; other servers still see publishes or saved snapshots. Acquiring or releasing ownership only notifies if the visible value changes. Release precedes the final save, so an older snapshot or null may be visible before the final value arrives.

## Values

A key's value is turned into bytes by a `ValueCodec<T>`:

```java
// built-in: ValueCodecs.BOOLEAN, INT, LONG, DOUBLE, STRING, UUID, BYTES
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
            out.write(ValueCodecs.STRING, home.server());   // added in v2
        },
        in -> new Home(
                in.read(ValueCodecs.STRING),
                in.read(ValueCodecs.DOUBLE),
                in.read(ValueCodecs.DOUBLE),
                in.read(ValueCodecs.DOUBLE),
                in.hasRemaining() ? in.read(ValueCodecs.STRING) : "lobby"   // v1 data has no server
        )
);
```

- The same value must always encode to the same bytes, and `decode(encode(v))` must equal `v`. Non-deterministic encoding stays correct but turns no-op updates into writes. Maps encode in iteration order, so use `LinkedHashMap` or `TreeMap` when that matters.
- `decode` may run on the Redis subscription thread, so keep it fast and thread-safe. It must not keep a reference to the input array.
- Decoded lists and maps are unmodifiable. Struct fields are length-prefixed and read in order; new fields go at the end, and readers ignore trailing data they don't know.
- Store composed codecs in constants: `list`, `map`, `optional` and `struct` create a new codec on every call.

## Decimal counters

`dbCounter`, `dbMapCounter` and `memoryLong` accept `scale(int)` (0 to 18). The key still stores and exchanges a `long` in minor units; the scale only drives conversion:

```java
long units = coins.toUnits(new BigDecimal("19.99"), RoundingMode.UNNECESSARY); // 1999
long fromVault = coins.toUnits(amount, RoundingMode.DOWN);                     // double input
BigDecimal shown = coins.toDecimal(units);                                    // 19.99, always `scale` digits
double forVault = coins.toDouble(units);
```

`toUnits(double, ...)` treats a double as the decimal it prints as, so `0.57` becomes 57. Arithmetic on doubles before converting still carries double errors. Never change the scale of a key that has data; existing values would be read 10^n times off.

## Limitations

- Redis and the database are assumed to be available. Lost messages and infrastructure failures are not compensated.
- An override that arrives while the holder is doing its final save on `offline` can be lost: the save writes the old value back, and the released holder drops the message.
- A map value nested inside an entry is stored as one value; map counter keys can't be ranked.