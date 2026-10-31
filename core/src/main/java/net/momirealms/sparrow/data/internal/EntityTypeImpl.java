package net.momirealms.sparrow.data.internal;

import com.mojang.serialization.Codec;
import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.RankEntry;
import net.momirealms.sparrow.data.entity.Entity;
import net.momirealms.sparrow.data.entity.EntityIdCodec;
import net.momirealms.sparrow.data.entity.EntityType;
import net.momirealms.sparrow.data.internal.db.DbOperations;
import net.momirealms.sparrow.data.internal.key.AbstractDataKey;
import net.momirealms.sparrow.data.internal.key.KeyRegistry;
import net.momirealms.sparrow.data.internal.key.MapKeyImpl;
import net.momirealms.sparrow.data.internal.key.ValueForm;
import net.momirealms.sparrow.data.internal.key.ValueKeyImpl;
import net.momirealms.sparrow.data.internal.key.builder.DbCounterKeyBuilderImpl;
import net.momirealms.sparrow.data.internal.key.builder.DbKeyBuilderImpl;
import net.momirealms.sparrow.data.internal.key.builder.DbMapCounterKeyBuilderImpl;
import net.momirealms.sparrow.data.internal.key.builder.DbMapKeyBuilderImpl;
import net.momirealms.sparrow.data.internal.key.builder.MemoryKeyBuilderImpl;
import net.momirealms.sparrow.data.internal.key.builder.MemoryLongKeyBuilderImpl;
import net.momirealms.sparrow.data.internal.key.builder.MemoryMapKeyBuilderImpl;
import net.momirealms.sparrow.data.internal.memory.Backfill;
import net.momirealms.sparrow.data.internal.memory.HeldEntity;
import net.momirealms.sparrow.data.internal.memory.OverrideService;
import net.momirealms.sparrow.data.internal.memory.ResidenceImpl;
import net.momirealms.sparrow.data.internal.snapshot.SnapshotCache;
import net.momirealms.sparrow.data.internal.sync.DataMessenger;
import net.momirealms.sparrow.data.key.DataKey;
import net.momirealms.sparrow.data.key.EntryKeyCodec;
import net.momirealms.sparrow.data.key.MemoryDataKey;
import net.momirealms.sparrow.data.key.SortableKey;
import net.momirealms.sparrow.data.key.builder.DbCounterKeyBuilder;
import net.momirealms.sparrow.data.key.builder.DbKeyBuilder;
import net.momirealms.sparrow.data.key.builder.DbMapCounterKeyBuilder;
import net.momirealms.sparrow.data.key.builder.DbMapKeyBuilder;
import net.momirealms.sparrow.data.key.builder.MemoryKeyBuilder;
import net.momirealms.sparrow.data.key.builder.MemoryLongKeyBuilder;
import net.momirealms.sparrow.data.key.builder.MemoryMapKeyBuilder;
import net.momirealms.sparrow.data.storage.EntityRef;
import net.momirealms.sparrow.data.storage.EntityStorage;
import net.momirealms.sparrow.data.storage.RankRow;
import net.momirealms.sparrow.data.storage.StoredValue;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@ApiStatus.Internal
public final class EntityTypeImpl<K> implements EntityType<K>, DataMessenger.Listener {
    private static final int MAX_ENTITY_ID_LENGTH = 64;

    private final DataServiceImpl service;
    private final String name;
    private final EntityIdCodec<K> idCodec;
    private final KeyRegistry<K> keys = new KeyRegistry<>();
    private final SnapshotCache<K> snapshots;
    private final DbOperations<K> db;
    private final Map<K, HeldEntity<K>> heldEntities = new ConcurrentHashMap<>();
    private final ResidenceImpl<K> residence;
    private final OverrideService<K> overrides;
    private final Backfill<K> backfill;

    // 一个实体类型在运行时的实现, 注册的键都登记在它自己的键表里. 它也是这个类型的消息接收方, 收到失效让对应的快照重读, 收到 publish 直接更新快照
    public EntityTypeImpl(@NotNull DataServiceImpl service, @NotNull String name, @NotNull EntityIdCodec<K> idCodec) {
        this.service = service;
        this.name = name;
        this.idCodec = idCodec;
        this.snapshots = new SnapshotCache<>(service.snapshotExpiry());
        this.db = new DbOperations<>(this, service.storage(), service.executor(), service.messenger());
        this.residence = new ResidenceImpl<>(this, service);
        this.overrides = new OverrideService<>(this, service);
        this.backfill = new Backfill<>(this);
    }

    @NotNull
    @Override
    public String name() {
        return this.name;
    }

    @NotNull
    @Override
    public Entity<K> entity(@NotNull K id) {
        return new EntityImpl<>(this, id);
    }

    @NotNull
    @Override
    public ResidenceImpl<K> residence() {
        return this.residence;
    }

    @NotNull
    @Override
    public <T> MemoryKeyBuilder<K, T> memory(@NotNull Key id, @NotNull Codec<T> codec, @NotNull T defaultValue) {
        return new MemoryKeyBuilderImpl<>(this, id, new ValueForm.NbtForm<>(codec, defaultValue));
    }

    @NotNull
    @Override
    public <T> MemoryKeyBuilder<K, Optional<T>> memory(@NotNull Key id, @NotNull Codec<T> codec) {
        return new MemoryKeyBuilderImpl<>(this, id, new ValueForm.OptionalNbtForm<>(codec));
    }

    @NotNull
    @Override
    public MemoryLongKeyBuilder<K> memoryLong(@NotNull Key id) {
        return new MemoryLongKeyBuilderImpl<>(this, id);
    }

    // 映射键的条目没有默认值, 条目不存在时读到 null
    @NotNull
    @Override
    public <E, V> MemoryMapKeyBuilder<K, E, V> memoryMap(@NotNull Key id, @NotNull EntryKeyCodec<E> entryCodec, @NotNull Codec<V> valueCodec) {
        return new MemoryMapKeyBuilderImpl<>(this, id, entryCodec, new ValueForm.NbtForm<>(valueCodec, null));
    }

    @NotNull
    @Override
    public <T> DbKeyBuilder<K, T> db(@NotNull Key id, @NotNull Codec<T> codec, @NotNull T defaultValue) {
        return new DbKeyBuilderImpl<>(this, id, new ValueForm.NbtForm<>(codec, defaultValue));
    }

    @NotNull
    @Override
    public <T> DbKeyBuilder<K, Optional<T>> db(@NotNull Key id, @NotNull Codec<T> codec) {
        return new DbKeyBuilderImpl<>(this, id, new ValueForm.OptionalNbtForm<>(codec));
    }

    @NotNull
    @Override
    public DbCounterKeyBuilder<K> dbCounter(@NotNull Key id) {
        return new DbCounterKeyBuilderImpl<>(this, id);
    }

    @NotNull
    @Override
    public <E, V> DbMapKeyBuilder<K, E, V> dbMap(@NotNull Key id, @NotNull EntryKeyCodec<E> entryCodec, @NotNull Codec<V> valueCodec) {
        return new DbMapKeyBuilderImpl<>(this, id, entryCodec, new ValueForm.NbtForm<>(valueCodec, null));
    }

    @NotNull
    @Override
    public <E> DbMapCounterKeyBuilder<K, E> dbMapCounter(@NotNull Key id, @NotNull EntryKeyCodec<E> entryCodec) {
        return new DbMapCounterKeyBuilderImpl<>(this, id, entryCodec);
    }

    // DB 键只需要移出键表. 内存键移出键表后, 每个持有中的实体把这个键排进落库链, 落库时摘掉槽, future 在全部落库后结束.
    // 两种键都丢掉本服这个键的快照, 库里的数据保留, 要删数据用 purge. 注销之后调用方不再使用旧的键对象
    @NotNull
    @Override
    public CompletableFuture<Void> unregister(@NotNull DataKey<K> key) {
        AbstractDataKey<K> impl = (AbstractDataKey<K>) key;
        String keyId = impl.storageKey().id();
        if (!(key instanceof MemoryDataKey<?>)) {
            this.keys.unregister(impl);
            this.snapshots.discard(keyId);
            return CompletableFuture.completedFuture(null);
        }
        Map<K, HeldEntity<K>> targets = this.overrides.beginUnregister(impl);
        this.snapshots.discard(keyId);
        CompletableFuture<?>[] saves = new CompletableFuture<?>[targets.size()];
        int index = 0;
        for (HeldEntity<K> held : targets.values()) {
            saves[index++] = held.detach(impl);
        }
        return CompletableFuture.allOf(saves);
    }

    // 跨所有实体删除一个键 ID 的数据, 用在功能永久下线. 本服还注册着这个 ID 时抛 IllegalStateException, 其他服先注销由调用方保证.
    // 删完广播这个键在所有实体上的失效
    @NotNull
    @Override
    public CompletableFuture<Void> purge(@NotNull Key id) {
        if (this.keys.get(id) != null) throw new IllegalStateException("Data key is still registered: " + id);
        String keyId = id.toString();
        return CompletableFuture.runAsync(() -> this.service.storage().purge(this.name, keyId), this.service.executor())
                .thenRun(() -> this.service.messenger().invalidate(this.name, null, keyId, null));
    }

    @NotNull
    @Override
    public CompletableFuture<List<RankEntry<K>>> top(@NotNull SortableKey<K> key, int offset, int limit) {
        ValueKeyImpl<K, Long> impl = (ValueKeyImpl<K, Long>) key;
        return CompletableFuture.supplyAsync(() -> {
            List<RankRow> rows = this.service.storage().top(this.name, impl.storageKey(), offset, limit);
            int size = rows.size();
            List<RankEntry<K>> entries = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                RankRow row = rows.get(i);
                entries.add(new RankEntry<>(this.idCodec.decode(row.entityId()), row.value()));
            }
            return entries;
        }, this.service.executor());
    }

    // 先拿到这个实体的数值, 存储里没有就用默认值, 所以从没落过库的实体也有名次. 名次是比它大的实体个数加一, 数值相同的名次相同
    @NotNull
    @Override
    public CompletableFuture<Long> rankOf(@NotNull SortableKey<K> key, @NotNull K id) {
        ValueKeyImpl<K, Long> impl = (ValueKeyImpl<K, Long>) key;
        EntityRef ref = this.ref(id);
        return CompletableFuture.supplyAsync(() -> {
            EntityStorage storage = this.service.storage();
            long value = impl.form().load(storage.read(ref, impl.storageKey(), null));
            return storage.countAbove(this.name, impl.storageKey(), value) + 1;
        }, this.service.executor());
    }

    // 在 Redis 的订阅线程上调用. 这里只解码实体 ID, 快照的重读在宿主的 Executor 上进行
    @Override
    public void invalidate(byte @Nullable [] entityId, @Nullable String keyId, @Nullable String entry) {
        this.snapshots.invalidate(entityId == null ? null : this.idCodec.decode(entityId), keyId, entry);
    }

    // 对快照来说覆盖和失效一样, 重读一次. 本服持有或者正在加载这个实体时, 还要把新值交给持有端
    @Override
    public void override(byte @NotNull [] entityId, @NotNull String keyId, @Nullable String entry, @Nullable StoredValue value) {
        K id = this.idCodec.decode(entityId);
        this.snapshots.invalidate(id, keyId, entry);
        this.overrides.receive(id, keyId, entry, value);
    }

    // 收到持有方 publish 的值. 本服在看这个对象时直接把值放进快照, 不重读库; 没人在看就不解码
    @Override
    public void publishValue(byte @NotNull [] entityId, @NotNull String keyId, @Nullable StoredValue value) {
        K id = this.idCodec.decode(entityId);
        if (!this.snapshots.isWatched(id, keyId)) return;
        if (!(this.keys.get(Key.of(keyId)) instanceof ValueKeyImpl<?, ?> key)) return;
        this.snapshots.put(id, keyId, null, key.form().load(value));
    }

    @Override
    public void publishEntries(byte @NotNull [] entityId, @NotNull String keyId, @NotNull Map<String, StoredValue> entries) {
        K id = this.idCodec.decode(entityId);
        if (!this.snapshots.isWatched(id, keyId)) return;
        if (!(this.keys.get(Key.of(keyId)) instanceof MapKeyImpl<?, ?, ?> key)) return;
        this.putEntries(id, keyId, key, entries);
    }

    // 编码后的实体 ID 超过 64 字节时抛 IllegalArgumentException, 宿主自定义的编码器也一样
    @NotNull
    public EntityRef ref(@NotNull K id) {
        byte[] encoded = this.idCodec.encode(id);
        if (encoded.length > MAX_ENTITY_ID_LENGTH) throw new IllegalArgumentException("Encoded entity id is longer than " + MAX_ENTITY_ID_LENGTH + " bytes: " + id);
        return new EntityRef(this.name, encoded);
    }

    // 本服持有这个实体时返回它的持有容器, 否则返回 null
    @Nullable
    public HeldEntity<K> held(@NotNull K id) {
        return this.heldEntities.get(id);
    }

    public void putHeld(@NotNull K id, @NotNull HeldEntity<K> held) {
        this.heldEntities.put(id, held);
    }

    @Nullable
    public HeldEntity<K> removeHeld(@NotNull K id) {
        return this.heldEntities.remove(id);
    }

    // 复制一份这一刻的持有表, 补读和注销用它决定要处理哪些实体
    @NotNull
    public Map<K, HeldEntity<K>> heldEntities() {
        return Map.copyOf(this.heldEntities);
    }

    @NotNull
    public DataServiceImpl service() {
        return this.service;
    }

    @NotNull
    public EntityIdCodec<K> idCodec() {
        return this.idCodec;
    }

    @NotNull
    public KeyRegistry<K> keys() {
        return this.keys;
    }

    @NotNull
    public SnapshotCache<K> snapshots() {
        return this.snapshots;
    }

    @NotNull
    public DbOperations<K> db() {
        return this.db;
    }

    @NotNull
    public OverrideService<K> overrides() {
        return this.overrides;
    }

    @NotNull
    public Backfill<K> backfill() {
        return this.backfill;
    }

    // 整个映射的快照用解码后的条目键, 单个条目的快照用编码后的条目键
    private <E, V> void putEntries(@NotNull K id, @NotNull String keyId, @NotNull MapKeyImpl<?, E, V> key, @NotNull Map<String, StoredValue> stored) {
        Map<String, V> entries = HashMap.newHashMap(stored.size());
        Map<E, V> all = HashMap.newHashMap(stored.size());
        stored.forEach((entry, value) -> {
            V decoded = key.valueForm().load(value);
            entries.put(entry, decoded);
            all.put(key.entryCodec().decode(entry), decoded);
        });
        this.snapshots.putEntries(id, keyId, Collections.unmodifiableMap(all), entries);
    }
}