package net.momirealms.sparrow.data.internal.memory;

import net.momirealms.sparrow.data.internal.db.DbOperations;
import net.momirealms.sparrow.data.internal.key.MapKeyImpl;
import net.momirealms.sparrow.data.internal.key.ValueKeyImpl;
import net.momirealms.sparrow.data.internal.redis.ReplicaStore;
import net.momirealms.sparrow.data.storage.EntityRef;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

@ApiStatus.Internal
public final class ReplicaReader<K> {
    private final ReplicaStore replicas;
    private final DbOperations<K> db;
    private final Executor executor;

    // 本服没有持有时读内存键, 先读 Redis 副本, 没有副本再读存储. 副本里的空值表示持有者发布的就是"没有值", 不回头读存储.
    // 副本的结果在 Executor 上解码, Redis 的 I/O 线程只负责收发
    public ReplicaReader(@NotNull ReplicaStore replicas, @NotNull DbOperations<K> db, @NotNull Executor executor) {
        this.replicas = replicas;
        this.db = db;
        this.executor = executor;
    }

    @NotNull
    public <T> CompletableFuture<T> read(@NotNull EntityRef ref, @NotNull ValueKeyImpl<K, T> key) {
        return this.replicas.readValue(ref, key.storageKey().id()).thenComposeAsync(replica -> {
            if (replica == null) return this.db.read(ref, key);
            return CompletableFuture.completedFuture(key.form().fromReplica(replica));
        }, this.executor);
    }

    // 副本里有标记却没有这个条目时是空值, 解码成 null, 表示条目不存在
    @NotNull
    public <V> CompletableFuture<@Nullable V> readEntry(@NotNull EntityRef ref, @NotNull MapKeyImpl<K, ?, V> key, @NotNull String entry) {
        return this.replicas.readEntry(ref, key.storageKey().id(), entry).thenComposeAsync(replica -> {
            if (replica == null) return this.db.readEntry(ref, key, entry);
            return CompletableFuture.completedFuture(key.valueForm().fromReplica(replica));
        }, this.executor);
    }

    @NotNull
    public <E, V> CompletableFuture<Map<E, V>> readAll(@NotNull EntityRef ref, @NotNull MapKeyImpl<K, E, V> key) {
        return this.replicas.readEntries(ref, key.storageKey().id()).thenComposeAsync(replica -> {
            if (replica == null) return this.db.readAll(ref, key);
            Map<E, V> entries = HashMap.newHashMap(replica.size());
            replica.forEach((entry, value) -> entries.put(key.entryCodec().decode(entry), key.valueForm().fromReplica(value)));
            return CompletableFuture.completedFuture(Collections.unmodifiableMap(entries));
        }, this.executor);
    }
}