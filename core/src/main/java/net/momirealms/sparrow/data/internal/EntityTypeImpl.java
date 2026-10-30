package net.momirealms.sparrow.data.internal;

import com.mojang.serialization.Codec;
import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.RankEntry;
import net.momirealms.sparrow.data.entity.Entity;
import net.momirealms.sparrow.data.entity.EntityIdCodec;
import net.momirealms.sparrow.data.entity.EntityType;
import net.momirealms.sparrow.data.entity.Residence;
import net.momirealms.sparrow.data.internal.db.DbOperations;
import net.momirealms.sparrow.data.internal.key.KeyRegistry;
import net.momirealms.sparrow.data.internal.key.ValueForm;
import net.momirealms.sparrow.data.internal.key.ValueKeyImpl;
import net.momirealms.sparrow.data.internal.key.builder.DbCounterKeyBuilderImpl;
import net.momirealms.sparrow.data.internal.key.builder.DbKeyBuilderImpl;
import net.momirealms.sparrow.data.internal.key.builder.DbMapCounterKeyBuilderImpl;
import net.momirealms.sparrow.data.internal.key.builder.DbMapKeyBuilderImpl;
import net.momirealms.sparrow.data.internal.key.builder.MemoryKeyBuilderImpl;
import net.momirealms.sparrow.data.internal.key.builder.MemoryLongKeyBuilderImpl;
import net.momirealms.sparrow.data.internal.key.builder.MemoryMapKeyBuilderImpl;
import net.momirealms.sparrow.data.internal.snapshot.SnapshotCache;
import net.momirealms.sparrow.data.internal.sync.DataMessenger;
import net.momirealms.sparrow.data.key.DataKey;
import net.momirealms.sparrow.data.key.EntryKeyCodec;
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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@ApiStatus.Internal
public final class EntityTypeImpl<K> implements EntityType<K>, DataMessenger.Listener {
    private static final int MAX_ENTITY_ID_LENGTH = 64;

    private final DataServiceImpl service;
    private final String name;
    private final EntityIdCodec<K> idCodec;
    private final KeyRegistry<K> keys = new KeyRegistry<>();
    private final SnapshotCache<K> snapshots;
    private final DbOperations<K> db;

    // 一个实体类型在运行时的实现, 注册的键都登记在它自己的键表里. 它也是这个类型的消息接收方, 收到失效就让对应的快照重读
    public EntityTypeImpl(@NotNull DataServiceImpl service, @NotNull String name, @NotNull EntityIdCodec<K> idCodec) {
        this.service = service;
        this.name = name;
        this.idCodec = idCodec;
        this.snapshots = new SnapshotCache<>(service.snapshotExpiry(), service.logger());
        this.db = new DbOperations<>(this, service.storage(), service.executor(), service.messenger());
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
    public Residence<K> residence() {
        throw new UnsupportedOperationException("Not implemented yet");
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

    @NotNull
    @Override
    public CompletableFuture<Void> unregister(@NotNull DataKey<K> key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @NotNull
    @Override
    public CompletableFuture<Void> purge(@NotNull Key id) {
        throw new UnsupportedOperationException("Not implemented yet");
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

    // 对快照来说覆盖和失效一样, 重读一次
    @Override
    public void override(byte @NotNull [] entityId, @NotNull String keyId, @Nullable String entry, @Nullable StoredValue value) {
        this.snapshots.invalidate(this.idCodec.decode(entityId), keyId, entry);
    }

    // 编码后的实体 ID 超过 64 字节时抛 IllegalArgumentException, 宿主自定义的编码器也一样
    @NotNull
    public EntityRef ref(@NotNull K id) {
        byte[] encoded = this.idCodec.encode(id);
        if (encoded.length > MAX_ENTITY_ID_LENGTH) throw new IllegalArgumentException("Encoded entity id is longer than " + MAX_ENTITY_ID_LENGTH + " bytes: " + id);
        return new EntityRef(this.name, encoded);
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
}