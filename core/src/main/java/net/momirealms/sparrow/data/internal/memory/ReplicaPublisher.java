package net.momirealms.sparrow.data.internal.memory;

import net.momirealms.sparrow.data.internal.key.AbstractDataKey;
import net.momirealms.sparrow.data.internal.redis.ReplicaStore;
import net.momirealms.sparrow.data.internal.sync.DataMessenger;
import net.momirealms.sparrow.data.storage.EntityRef;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

@ApiStatus.Internal
public final class ReplicaPublisher {
    private final ReplicaStore replicas;
    private final DataMessenger<?> messenger;
    private final Executor executor;
    private final long refreshNanos;

    // 把持有容器里的值写进 Redis 副本. 写副本和广播走不同的连接, 所以等副本写完再广播失效, 观察方收到失效时读到的就是新副本.
    // 距上次 publish 超过 TTL 的一半时, 副本可能快过期了, 这时映射键整份重写, 没改过的键也重写一次来刷新 TTL
    public ReplicaPublisher(@NotNull ReplicaStore replicas, @NotNull DataMessenger<?> messenger, @NotNull Executor executor, @NotNull Duration replicaTtl) {
        this.replicas = replicas;
        this.messenger = messenger;
        this.executor = executor;
        this.refreshNanos = replicaTtl.toNanos() / 2;
    }

    // 轮到这个键时调用. 在 Executor 上取值和编码, 返回的 future 在副本写完, 失效发出之后结束
    @NotNull
    <K> CompletableFuture<Void> publish(@NotNull HeldEntity<K> held, @NotNull AbstractDataKey<K> key) {
        return CompletableFuture.supplyAsync(() -> this.send(held.ref(), held.takePublication(key, System.nanoTime(), this.refreshNanos)), this.executor)
                .thenCompose(Function.identity());
    }

    @NotNull
    CompletableFuture<Void> publishAll(@NotNull HeldEntity<?> held) {
        return CompletableFuture.supplyAsync(() -> this.send(held.ref(), held.takePublications(System.nanoTime(), this.refreshNanos)), this.executor)
                .thenCompose(Function.identity());
    }

    // 在 Executor 的线程上编码, 然后发出命令, 各个键的副本互不等待
    @NotNull
    private CompletableFuture<Void> send(@NotNull EntityRef ref, @NotNull HeldEntity.Publications publications) {
        List<HeldValue.Publication<?>> values = publications.values();
        List<HeldEntries.Publication<?>> entries = publications.entries();
        CompletableFuture<?>[] sent = new CompletableFuture<?>[values.size() + entries.size()];
        int index = 0;
        int valueCount = values.size();
        for (int i = 0; i < valueCount; i++) {
            sent[index++] = this.sendValue(ref, values.get(i));
        }
        int entryCount = entries.size();
        for (int i = 0; i < entryCount; i++) {
            sent[index++] = this.sendEntries(ref, entries.get(i));
        }
        return CompletableFuture.allOf(sent);
    }

    @NotNull
    private <T> CompletableFuture<Void> sendValue(@NotNull EntityRef ref, @NotNull HeldValue.Publication<T> publication) {
        String keyId = publication.key().id();
        return this.replicas.writeValue(ref, keyId, publication.form().replica(publication.value()))
                .thenRun(() -> this.messenger.invalidate(ref.entityType(), ref.entityId(), keyId, null));
    }

    // 整份重写时广播整个键的失效, 不用一次发几百条; 增量时只让观察方重读写到和删掉的条目
    @NotNull
    private <V> CompletableFuture<Void> sendEntries(@NotNull EntityRef ref, @NotNull HeldEntries.Publication<V> publication) {
        String keyId = publication.key().id();
        Map<String, byte[]> changed = HashMap.newHashMap(publication.written().size());
        publication.written().forEach((entry, value) -> changed.put(entry, publication.form().replica(value)));
        return this.replicas.writeEntries(ref, keyId, changed, publication.removed()).thenRun(() -> {
            if (publication.full()) {
                this.messenger.invalidate(ref.entityType(), ref.entityId(), keyId, null);
                return;
            }
            for (String entry : changed.keySet()) {
                this.messenger.invalidate(ref.entityType(), ref.entityId(), keyId, entry);
            }
            List<String> removed = publication.removed();
            int size = removed.size();
            for (int i = 0; i < size; i++) {
                this.messenger.invalidate(ref.entityType(), ref.entityId(), keyId, removed.get(i));
            }
        });
    }
}