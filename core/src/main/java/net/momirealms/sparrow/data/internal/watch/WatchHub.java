package net.momirealms.sparrow.data.internal.watch;

import net.momirealms.sparrow.data.entity.Watch;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.internal.key.AbstractDataKey;
import net.momirealms.sparrow.data.internal.key.ValueKeyImpl;
import net.momirealms.sparrow.data.internal.memory.HeldEntity;
import net.momirealms.sparrow.data.internal.snapshot.SingleFlightLoader;
import net.momirealms.sparrow.data.internal.snapshot.SnapshotCache;
import net.momirealms.sparrow.data.internal.util.Values;
import net.momirealms.sparrow.data.key.MemoryDataKey;
import net.momirealms.sparrow.data.key.ValueKey;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

@ApiStatus.Internal
public final class WatchHub<K> implements SnapshotCache.Observer<K> {
    private static final Logger LOGGER = LoggerFactory.getLogger(WatchHub.class);
    private static final Object UNLOADED = new Object();

    private final EntityTypeImpl<K> type;
    private final Map<K, Map<String, Node<?>>> entities = new ConcurrentHashMap<>();  // 实体 -> 键 ID -> 节点, 增删都在外层的 compute 里

    // 一个实体类型的 watch 和 pin. 能看到的值是本服持有时持有容器里的值, 其他情况是快照里的值, 和上次通知时不 equals 才通知.
    // 所有改动点只调用 touch, 由这里重新取值比较. 回调不带值, 在触发改动的线程上同步执行. onChange 为 null 的是 pin, 只固定快照, 不比较也不通知
    public WatchHub(@NotNull EntityTypeImpl<K> type) {
        this.type = type;
    }

    // 先登记再让快照加载, 加载完成时的 touch 一定能找到这个节点. 节点第一次建出来时记下当前看到的值, 之后变了才通知.
    // 登记之后已经能读到值时 ready 当场完成, 否则由首次读到值的 refresh 完成
    @NotNull
    public <T> Watch watch(@NotNull K id, @NotNull ValueKey<K, T> key, @Nullable Runnable onChange) {
        String keyId = ((AbstractDataKey<K>) key).storageKey().id();
        Binding binding = new Binding(id, keyId, onChange);
        this.entities.compute(id, (ignored, nodes) -> {
            Map<String, Node<?>> watched = nodes == null ? new ConcurrentHashMap<>() : nodes;
            watched.computeIfAbsent(keyId, created -> new Node<>(id, keyId, key)).attach(binding);
            return watched;
        });
        if (binding.node.visible()) {
            binding.ready.complete(null);
        }
        binding.node.load();
        return binding;
    }

    // 这个实体上这个键的值可能变了
    public void touch(@NotNull K id, @NotNull String keyId) {
        if (this.entities.isEmpty()) return;
        Map<String, Node<?>> nodes = this.entities.get(id);
        if (nodes == null) return;
        Node<?> node = nodes.get(keyId);
        if (node != null) {
            node.refresh();
        }
    }

    // 实体放进持有表或者移出持有表之后调用, 能看到的值换了来源
    public void touchEntity(@NotNull K id) {
        if (this.entities.isEmpty()) return;
        Map<String, Node<?>> nodes = this.entities.get(id);
        if (nodes == null) return;
        for (Node<?> node : nodes.values()) {
            node.refresh();
        }
    }

    // 持有方下线时调用, 在容器移出持有表之前. 被观察的内存键把持有的值写进快照, 移出之后能看到的值不变
    public void handOff(@NotNull K id) {
        Map<String, Node<?>> nodes = this.entities.get(id);
        if (nodes == null) return;
        for (Node<?> node : nodes.values()) {
            node.handOff();
        }
    }

    // 注销键时调用. 观察这个键的 watch 都关掉, 每个再通知最后一次
    public void discard(@NotNull String keyId) {
        List<Binding> retired = new ArrayList<>();
        for (K id : this.entities.keySet()) {
            this.entities.computeIfPresent(id, (ignored, nodes) -> {
                Node<?> node = nodes.remove(keyId);
                if (node != null) {
                    retired.addAll(node.bindings);
                }
                return nodes.isEmpty() ? null : nodes;
            });
        }
        for (int i = 0, size = retired.size(); i < size; i++) {
            Binding binding = retired.get(i);
            if (binding.closed.compareAndSet(false, true)) {
                binding.cancel("Watched key was unregistered before the watch was ready");
                binding.notifyChange();
            }
        }
    }

    // 关闭数据层时调用, 关掉全部 watch, 不再通知
    public void clear() {
        for (Map<String, Node<?>> nodes : this.entities.values()) {
            for (Node<?> node : nodes.values()) {
                for (Binding binding : node.bindings) {
                    binding.closed.set(true);
                    binding.cancel("Data service was closed before the watch was ready");
                }
            }
        }
        this.entities.clear();
    }

    @Override
    public boolean pinned(@NotNull K id, @NotNull String keyId) {
        Map<String, Node<?>> nodes = this.entities.get(id);
        return nodes != null && nodes.containsKey(keyId);
    }

    @Override
    public void replaced(@NotNull K id, @NotNull String keyId) {
        this.touch(id, keyId);
    }

    // 读取 future 给出的是包着存储异常的 CompletionException, 交给 ready 的是里面的存储异常
    @Override
    public void failed(@NotNull K id, @NotNull String keyId, @NotNull Throwable error) {
        Map<String, Node<?>> nodes = this.entities.get(id);
        if (nodes == null) return;
        Node<?> node = nodes.get(keyId);
        if (node != null) {
            node.fail(error instanceof CompletionException completion ? completion.getCause() : error);
        }
    }

    // 最后一个 watch 关闭时摘掉节点, 这个键在这个实体上没有观察了就让快照恢复过期
    private void unwatch(@NotNull Binding binding) {
        K id = binding.id;
        String keyId = binding.keyId;
        this.entities.computeIfPresent(id, (ignored, nodes) -> {
            Node<?> node = nodes.get(keyId);
            if (node != null && node.detach(binding)) {
                nodes.remove(keyId);
            }
            return nodes.isEmpty() ? null : nodes;
        });
        if (!this.pinned(id, keyId)) {
            this.type.snapshots().unpin(id, keyId);
        }
    }

    // 一个实体的一个键和挂在它上面的 watch 与 pin. seen 是最近一次看到的值, UNLOADED 表示快照还没加载.
    // 内存键在本服持有时看的是持有容器, 其他情况看快照
    private final class Node<T> {
        private final K id;
        private final String keyId;
        private final ValueKey<K, T> key;
        private final boolean memory;
        private final Supplier<CompletableFuture<T>> reader;
        private final CopyOnWriteArrayList<Binding> bindings = new CopyOnWriteArrayList<>();
        private @Nullable Object seen;
        private volatile int watchers; // 带回调的绑定个数, 增减都在外层的 compute 里

        // 实体 ID 编码后超过 64 字节时 reader 抛 IllegalArgumentException, 在调用 watch 的线程上直接抛给调用方
        private Node(@NotNull K id, @NotNull String keyId, @NotNull ValueKey<K, T> key) {
            this.id = id;
            this.keyId = keyId;
            this.key = key;
            this.memory = key instanceof MemoryDataKey<?>;
            this.reader = WatchHub.this.type.reader(id, key);
            this.seen = this.observe();
        }

        // 在外层的 compute 里调用, 节点这时一定还在表里
        private void attach(@NotNull Binding binding) {
            this.bindings.add(binding);
            binding.node = this;
            if (binding.onChange != null) {
                this.watchers++;
            }
        }

        // 在外层的 compute 里调用, 返回节点上是不是已经没有绑定
        private boolean detach(@NotNull Binding binding) {
            if (!this.bindings.remove(binding)) return false;
            if (binding.onChange != null) {
                this.watchers--;
            }
            return this.bindings.isEmpty();
        }

        private synchronized boolean visible() {
            return this.seen != UNLOADED;
        }

        // 取值和比较在锁里, 并发的 touch 按顺序推进 seen; 回调在锁外. 第一次读到值时先让 ready 完成, 再通知.
        // 节点上只有 pin 时不比较, seen 照样跟到最新, 之后加进来的 watch 从这个值算起
        private void refresh() {
            List<Binding> listeners;
            boolean first;
            synchronized (this) {
                Object current = this.observe();
                // 快照还没加载时不通知, 持有方刚下线时能看到的值不会先变成空
                if (current == UNLOADED) return;
                first = this.seen == UNLOADED;
                boolean changed = first || (this.watchers > 0 && !Values.same(this.seen, current));
                this.seen = current;
                if (!changed) return;
                listeners = List.copyOf(this.bindings);
            }
            int size = listeners.size();
            if (first) {
                for (int i = 0; i < size; i++) {
                    listeners.get(i).ready.complete(null);
                }
            }
            for (int i = 0; i < size; i++) {
                Binding binding = listeners.get(i);
                if (!binding.closed.get()) {
                    binding.notifyChange();
                }
            }
        }

        // 首次加载失败时让还在等的 ready 失败. 已经读到过值时不受影响, 之后新加的 watch 会再发起一次加载
        private void fail(@NotNull Throwable error) {
            List<Binding> waiting;
            synchronized (this) {
                if (this.seen != UNLOADED) return;
                waiting = List.copyOf(this.bindings);
            }
            int size = waiting.size();
            for (int i = 0; i < size; i++) {
                waiting.get(i).ready.completeExceptionally(error);
            }
        }

        @Nullable
        private HeldEntity<K> held() {
            return this.memory ? WatchHub.this.type.held(this.id) : null;
        }

        // 当前能看到的值, UNLOADED 表示快照还没加载
        @Nullable
        private Object observe() {
            HeldEntity<K> held = this.held();
            if (held != null) return held.get((ValueKeyImpl<K, T>) this.key);
            SingleFlightLoader.Loaded<T> loaded = WatchHub.this.type.snapshots().loaded(this.id, this.keyId);
            return loaded == null ? UNLOADED : loaded.value();
        }

        // 本服不持有时让快照加载, 也让快照节点按有人观察重新计算过期
        private void load() {
            if (this.held() != null) return;
            WatchHub.this.type.snapshots().peek(this.id, this.keyId, this.reader);
        }

        private void handOff() {
            HeldEntity<K> held = this.held();
            if (held == null) return;
            WatchHub.this.type.snapshots().seed(this.id, this.keyId, this.reader, held.get((ValueKeyImpl<K, T>) this.key));
        }
    }

    private final class Binding implements Watch {
        private final K id;
        private final String keyId;
        private final @Nullable Runnable onChange;
        private final AtomicBoolean closed = new AtomicBoolean();
        private final CompletableFuture<Void> ready = new CompletableFuture<>();
        private Node<?> node;

        private Binding(@NotNull K id, @NotNull String keyId, @Nullable Runnable onChange) {
            this.id = id;
            this.keyId = keyId;
            this.onChange = onChange;
        }

        @NotNull
        @Override
        public CompletableFuture<Void> ready() {
            return this.ready;
        }

        @Override
        public void close() {
            if (this.closed.compareAndSet(false, true)) {
                WatchHub.this.unwatch(this);
                this.cancel("Watch was closed before it was ready");
            }
        }

        // ready 已经完成时没有效果. CancellationException 带上原因, isCancelled 照样为 true
        private void cancel(@NotNull String reason) {
            this.ready.completeExceptionally(new CancellationException(reason));
        }

        // 回调来自宿主, 它的异常在这里记日志, 不影响其他 watch 和触发改动的调用方. pin 没有回调
        private void notifyChange() {
            if (this.onChange == null) return;
            try {
                this.onChange.run();
            } catch (RuntimeException exception) {
                LOGGER.error("Failed to notify a watch on {} {}", this.id, this.keyId, exception);
            }
        }
    }
}