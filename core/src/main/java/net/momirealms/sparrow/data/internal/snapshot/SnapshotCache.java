package net.momirealms.sparrow.data.internal.snapshot;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

@ApiStatus.Internal
public final class SnapshotCache<K> {
    private static final Duration PINNED = Duration.ofNanos(Long.MAX_VALUE);

    private final Cache<Node<K>, SingleFlightLoader<?>> nodes;
    private final Observer<K> observer;
    private final Map<K, Fence<K>> fences = new ConcurrentHashMap<>();

    // 一个实体类型的快照. 一个实体的一个键是一个节点, 按最近一次 peek 计时过期, 有人 watch 或 pin 的节点不过期.
    // 失效和本服写入用 getIfPresentQuietly 查找, 不算访问, 没人看的快照照样会过期.
    // 快照里的值每次被换掉都告诉 observer, 由它判断值有没有变
    public SnapshotCache(@NotNull Duration expireAfterAccess, @NotNull Observer<K> observer) {
        this.observer = observer;
        this.nodes = Caffeine.newBuilder()
                .expireAfter(Expiry.<Node<K>, SingleFlightLoader<?>>accessing((node, flight) -> observer.pinned(node.id(), node.keyId()) ? PINNED : expireAfterAccess))
                .build();
    }

    @Nullable
    public <V> V peek(@NotNull K id, @NotNull String keyId, @NotNull Supplier<CompletableFuture<V>> loader) {
        return this.flight(id, keyId, loader).peek();
    }

    // 只查询, 不触发加载, 也不算访问. 还没加载过时返回 null
    @SuppressWarnings("unchecked")
    @Nullable
    public <V> SingleFlightLoader.Loaded<V> loaded(@NotNull K id, @NotNull String keyId) {
        SingleFlightLoader<V> flight = (SingleFlightLoader<V>) this.nodes.policy().getIfPresentQuietly(new Node<>(id, keyId));
        return flight == null ? null : flight.loaded();
    }

    // 本服写入后用返回值更新快照. 没人看过的对象不建快照
    @SuppressWarnings("unchecked")
    public <V> void put(@NotNull K id, @NotNull String keyId, @Nullable V value) {
        SingleFlightLoader<V> flight = (SingleFlightLoader<V>) this.nodes.policy().getIfPresentQuietly(new Node<>(id, keyId));
        if (flight != null) {
            flight.put(value);
        }
    }

    // 持有方下线时把内存键的值交给快照. 和 put 不同, 快照里没有这个对象时也建出来, 之后由失效和 peek 照常维护
    public <V> void seed(@NotNull K id, @NotNull String keyId, @NotNull Supplier<CompletableFuture<V>> loader, @Nullable V value) {
        this.flight(id, keyId, loader).put(value);
    }

    // 只查询, 不算访问. 收到 publish 时先用它判断有没有人在看, 没人看就不解码
    public boolean isWatched(@NotNull K id, @NotNull String keyId) {
        return this.nodes.policy().getIfPresentQuietly(new Node<>(id, keyId)) != null;
    }

    // 范围和失效消息相同: keyId 为空是这个实体的全部键; id 为空是这个键在所有实体上.
    // 快照里没有的对象没人在看, 不读. 被栅栏挡住的对象先记下, 撤掉栅栏时再读
    public void invalidate(@Nullable K id, @Nullable String keyId) {
        if (id != null && keyId != null) {
            Node<K> node = new Node<>(id, keyId);
            SingleFlightLoader<?> flight = this.nodes.policy().getIfPresentQuietly(node);
            if (flight != null && !this.deferred(node)) {
                flight.invalidate();
            }
            return;
        }
        // 删除整个实体或清除一个键时才会走到这里, 很少发生, 遍历全部节点
        this.nodes.asMap().forEach((node, flight) -> {
            if ((id == null || node.id().equals(id)) && (keyId == null || node.keyId().equals(keyId)) && !this.deferred(node)) {
                flight.invalidate();
            }
        });
    }

    // 重读一个键的快照, future 在这次调用之后发起的那一轮读取落进快照时完成, 读取失败时带着异常失败.
    // 快照里没有这个对象时没人在看, 不读, 立即完成
    @NotNull
    public CompletableFuture<Void> reload(@NotNull K id, @NotNull String keyId) {
        SingleFlightLoader<?> flight = this.nodes.policy().getIfPresentQuietly(new Node<>(id, keyId));
        return flight == null ? CompletableFuture.completedFuture(null) : flight.reload();
    }

    // 持有方下线落库期间, 这个实体这些键的快照不重读. 这段时间读到的可能是落库之前的旧值, 收到的失效等 unfence 时一起处理
    public void fence(@NotNull K id, @NotNull Set<String> keyIds) {
        this.fences.put(id, new Fence<>(keyIds));
    }

    public void unfence(@NotNull K id) {
        Fence<K> fence = this.fences.remove(id);
        if (fence == null) return;
        List<Node<K>> pending = fence.close();
        for (int i = 0, size = pending.size(); i < size; i++) {
            SingleFlightLoader<?> flight = this.nodes.policy().getIfPresentQuietly(pending.get(i));
            if (flight != null) {
                flight.invalidate();
            }
        }
    }

    // 最后一个 watch 关闭后调用一次读访问, 让这个节点重新按 peek 计时过期
    public void unpin(@NotNull K id, @NotNull String keyId) {
        this.nodes.getIfPresent(new Node<>(id, keyId));
    }

    // 注销键时调用, 丢掉这个键在所有实体上的快照, 不重读. 在途的读取完成后写进的是已经移除的节点, 不会再被访问
    public void discard(@NotNull String keyId) {
        this.nodes.asMap().keySet().removeIf(node -> node.keyId().equals(keyId));
    }

    private boolean deferred(@NotNull Node<K> node) {
        Fence<K> fence = this.fences.get(node.id());
        return fence != null && fence.defer(node);
    }

    @SuppressWarnings("unchecked")
    @NotNull
    private <V> SingleFlightLoader<V> flight(@NotNull K id, @NotNull String keyId, @NotNull Supplier<CompletableFuture<V>> loader) {
        return (SingleFlightLoader<V>) this.nodes.get(new Node<>(id, keyId), node -> new SingleFlightLoader<>(
                loader,
                id + " " + keyId,
                () -> this.observer.replaced(id, keyId),
                error -> this.observer.failed(id, keyId, error)
        ));
    }

    // 快照的使用方. pinned 决定节点要不要固定不过期, replaced 在某个快照的值被换掉后调用
    public interface Observer<K> {

        boolean pinned(@NotNull K id, @NotNull String keyId);

        void replaced(@NotNull K id, @NotNull String keyId);

        // 某个快照的读取失败后调用, error 是读取 future 给出的异常
        void failed(@NotNull K id, @NotNull String keyId, @NotNull Throwable error);
    }

    private record Node<K>(@NotNull K id, @NotNull String keyId) {
    }

    // 一个实体落库期间的栅栏. 关闭之后不再接收, 失效照常处理
    private static final class Fence<K> {
        private final Set<String> keyIds;
        private final Set<Node<K>> pending = new HashSet<>();
        private boolean closed;

        private Fence(@NotNull Set<String> keyIds) {
            this.keyIds = keyIds;
        }

        private synchronized boolean defer(@NotNull Node<K> node) {
            if (this.closed || !this.keyIds.contains(node.keyId())) return false;
            this.pending.add(node);
            return true;
        }

        @NotNull
        private synchronized List<Node<K>> close() {
            this.closed = true;
            return new ArrayList<>(this.pending);
        }
    }
}