package net.momirealms.sparrow.data.internal.watch;

import net.momirealms.sparrow.data.entity.Watch;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.internal.key.MapKeyImpl;
import net.momirealms.sparrow.data.internal.key.ValueKeyImpl;
import net.momirealms.sparrow.data.internal.memory.HeldEntity;
import net.momirealms.sparrow.data.internal.snapshot.SingleFlightLoader;
import net.momirealms.sparrow.data.internal.snapshot.SnapshotCache;
import net.momirealms.sparrow.data.key.MemoryDataKey;
import net.momirealms.sparrow.data.storage.EntityRef;
import net.momirealms.sparrow.data.storage.StoredValue;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

@ApiStatus.Internal
public final class WatchHub<K> implements SnapshotCache.Observer<K> {
    private static final Logger LOGGER = LoggerFactory.getLogger(WatchHub.class);
    private static final Object UNLOADED = new Object();

    private final EntityTypeImpl<K> type;
    private final Map<K, Map<Target, Node>> entities = new ConcurrentHashMap<>();  // 实体 -> 观察目标 -> 节点, 增删都在外层的 compute 里

    // 一个实体类型的 watch. 能看到的值是本服持有时持有容器里的值, 其他情况是快照里的值, 编码之后和上次通知时的字节不同才通知.
    // 所有改动点只调用 touch, 由这里重新取值比较. 回调不带值, 在触发改动的线程上同步执行
    public WatchHub(@NotNull EntityTypeImpl<K> type) {
        this.type = type;
    }

    @NotNull
    public <T> Watch watch(@NotNull K id, @NotNull ValueKeyImpl<K, T> key, @NotNull Runnable onChange) {
        return this.watch(id, new ValueSource<>(id, key), onChange);
    }

    @NotNull
    public <V> Watch watch(@NotNull K id, @NotNull MapKeyImpl<K, ?, V> key, @NotNull String entry, @NotNull Runnable onChange) {
        return this.watch(id, new EntrySource<>(id, key, entry), onChange);
    }

    // 普通键的值, 或者映射键的一个条目可能变了. entry 为空时是这个键在这个实体上的全部观察
    public void touch(@NotNull K id, @NotNull String keyId, @Nullable String entry) {
        if (this.entities.isEmpty()) return;
        Map<Target, Node> targets = this.entities.get(id);
        if (targets == null) return;
        if (entry != null) {
            Node node = targets.get(new Target(keyId, entry));
            if (node != null) {
                node.refresh();
            }
            return;
        }
        for (Map.Entry<Target, Node> watched : targets.entrySet()) {
            if (watched.getKey().keyId().equals(keyId)) {
                watched.getValue().refresh();
            }
        }
    }

    // 实体放进持有表或者移出持有表之后调用, 能看到的值换了来源
    public void touchEntity(@NotNull K id) {
        if (this.entities.isEmpty()) return;
        Map<Target, Node> targets = this.entities.get(id);
        if (targets == null) return;
        for (Node node : targets.values()) {
            node.refresh();
        }
    }

    // 持有方下线时调用, 在容器移出持有表之前. 被观察的内存键把持有的值写进快照, 移出之后能看到的值不变
    public void handOff(@NotNull K id) {
        Map<Target, Node> targets = this.entities.get(id);
        if (targets == null) return;
        for (Node node : targets.values()) {
            node.source.handOff();
        }
    }

    // 注销键时调用. 观察这个键的 watch 都关掉, 每个再通知最后一次
    public void discard(@NotNull String keyId) {
        List<Binding> retired = new ArrayList<>();
        for (K id : this.entities.keySet()) {
            this.entities.computeIfPresent(id, (ignored, targets) -> {
                Iterator<Map.Entry<Target, Node>> iterator = targets.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<Target, Node> watched = iterator.next();
                    if (!watched.getKey().keyId().equals(keyId)) continue;
                    retired.addAll(watched.getValue().bindings);
                    iterator.remove();
                }
                return targets.isEmpty() ? null : targets;
            });
        }
        for (int i = 0, size = retired.size(); i < size; i++) {
            Binding binding = retired.get(i);
            if (binding.closed.compareAndSet(false, true)) {
                binding.notifyChange();
            }
        }
    }

    // 关闭数据层时调用, 关掉全部 watch, 不再通知
    public void clear() {
        for (Map<Target, Node> targets : this.entities.values()) {
            for (Node node : targets.values()) {
                for (Binding binding : node.bindings) {
                    binding.closed.set(true);
                }
            }
        }
        this.entities.clear();
    }

    @Override
    public boolean pinned(@NotNull K id, @NotNull String keyId) {
        Map<Target, Node> targets = this.entities.get(id);
        if (targets == null) return false;
        for (Target target : targets.keySet()) {
            if (target.keyId().equals(keyId)) return true;
        }
        return false;
    }

    @Override
    public void replaced(@NotNull K id, @NotNull String keyId, @Nullable String entry) {
        this.touch(id, keyId, entry);
    }

    // 先登记再让快照加载, 加载完成时的 touch 一定能找到这个节点. 节点第一次建出来时记下当前看到的值, 之后变了才通知
    @NotNull
    private Watch watch(@NotNull K id, @NotNull Source source, @NotNull Runnable onChange) {
        Target target = new Target(source.keyId, source.entry);
        Binding binding = new Binding(id, target, onChange);
        this.entities.compute(id, (ignored, targets) -> {
            Map<Target, Node> watched = targets == null ? new ConcurrentHashMap<>() : targets;
            watched.computeIfAbsent(target, created -> new Node(source)).bindings.add(binding);
            return watched;
        });
        source.load();
        return binding;
    }

    // 最后一个 watch 关闭时摘掉节点, 这个键在这个实体上没有观察了就让快照恢复过期
    private void unwatch(@NotNull Binding binding) {
        K id = binding.id;
        Target target = binding.target;
        this.entities.computeIfPresent(id, (ignored, targets) -> {
            Node node = targets.get(target);
            if (node != null && node.bindings.remove(binding) && node.bindings.isEmpty()) {
                targets.remove(target);
            }
            return targets.isEmpty() ? null : targets;
        });
        if (!this.pinned(id, target.keyId())) {
            this.type.snapshots().unpin(id, target.keyId());
        }
    }

    private static boolean same(@Nullable Object seen, @Nullable Object current) {
        if (seen == current) return true;
        if (seen instanceof StoredValue.OfBytes(byte[] before) && current instanceof StoredValue.OfBytes(byte[] after)) return Arrays.equals(before, after);
        return seen != null && seen.equals(current);
    }

    // 观察的对象. entry 为空是普通键的值
    private record Target(@NotNull String keyId, @Nullable String entry) {
    }

    // 一个观察对象和观察它的 watch. seen 是上次通知时看到的值: 编码后的 StoredValue, null 表示没有值, UNLOADED 表示快照还没加载
    private final class Node {
        private final Source source;
        private final CopyOnWriteArrayList<Binding> bindings = new CopyOnWriteArrayList<>();
        private @Nullable Object seen;

        private Node(@NotNull Source source) {
            this.source = source;
            this.seen = source.observe();
        }

        // 取值和比较在锁里, 并发的 touch 按顺序推进 seen; 回调在锁外
        private void refresh() {
            List<Binding> listeners;
            synchronized (this) {
                Object current = this.source.observe();
                // 快照还没加载时不通知, 持有方刚下线时能看到的值不会先变成空
                if (current == UNLOADED || same(this.seen, current)) return;
                this.seen = current;
                listeners = List.copyOf(this.bindings);
            }
            for (int i = 0, size = listeners.size(); i < size; i++) {
                Binding binding = listeners.get(i);
                if (!binding.closed.get()) {
                    binding.notifyChange();
                }
            }
        }
    }

    private final class Binding implements Watch {
        private final K id;
        private final Target target;
        private final Runnable onChange;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Binding(@NotNull K id, @NotNull Target target, @NotNull Runnable onChange) {
            this.id = id;
            this.target = target;
            this.onChange = onChange;
        }

        @Override
        public void close() {
            if (this.closed.compareAndSet(false, true)) {
                WatchHub.this.unwatch(this);
            }
        }

        // 回调来自宿主, 它的异常在这里记日志, 不影响其他 watch 和触发改动的调用方
        private void notifyChange() {
            try {
                this.onChange.run();
            } catch (RuntimeException exception) {
                LOGGER.error("Failed to notify a watch on {} {}", this.id, this.target, exception);
            }
        }
    }

    // 能看到的值从哪里来. 内存键在本服持有时取持有容器, 其他情况取快照
    private abstract class Source {
        protected final K id;
        protected final String keyId;
        protected final @Nullable String entry;
        protected final boolean memory;
        protected final EntityRef ref;

        // 实体 ID 编码后超过 64 字节时 ref 抛 IllegalArgumentException, 在调用 watch 的线程上直接抛给调用方
        protected Source(@NotNull K id, @NotNull String keyId, @Nullable String entry, boolean memory) {
            this.id = id;
            this.keyId = keyId;
            this.entry = entry;
            this.memory = memory;
            this.ref = WatchHub.this.type.ref(id);
        }

        @Nullable
        protected final HeldEntity<K> held() {
            return this.memory ? WatchHub.this.type.held(this.id) : null;
        }

        // 编码后的 StoredValue, null 表示没有值, UNLOADED 表示快照还没加载
        @Nullable
        protected abstract Object observe();

        // 本服不持有时让快照加载, 也让快照节点按有人观察重新计算过期
        protected abstract void load();

        protected abstract void handOff();
    }

    private final class ValueSource<T> extends Source {
        private final ValueKeyImpl<K, T> key;

        private ValueSource(@NotNull K id, @NotNull ValueKeyImpl<K, T> key) {
            super(id, key.storageKey().id(), null, key instanceof MemoryDataKey<?>);
            this.key = key;
        }

        @Nullable
        @Override
        protected Object observe() {
            HeldEntity<K> held = this.held();
            if (held != null) return this.key.form().store(held.get(this.key));
            SingleFlightLoader.Loaded<T> loaded = WatchHub.this.type.snapshots().loaded(this.id, this.keyId, null);
            if (loaded == null) return UNLOADED;
            T value = loaded.value();
            return value == null ? null : this.key.form().store(value);
        }

        @Override
        protected void load() {
            if (this.held() != null) return;
            WatchHub.this.type.snapshots().peek(this.id, this.keyId, () -> WatchHub.this.type.db().read(this.ref, this.key));
        }

        @Override
        protected void handOff() {
            HeldEntity<K> held = this.held();
            if (held == null) return;
            WatchHub.this.type.snapshots().seed(this.id, this.keyId, null, () -> WatchHub.this.type.db().read(this.ref, this.key), held.get(this.key));
        }
    }

    private final class EntrySource<V> extends Source {
        private final MapKeyImpl<K, ?, V> key;

        private EntrySource(@NotNull K id, @NotNull MapKeyImpl<K, ?, V> key, @NotNull String entry) {
            super(id, key.storageKey().id(), entry, key instanceof MemoryDataKey<?>);
            this.key = key;
        }

        @Nullable
        @Override
        protected Object observe() {
            HeldEntity<K> held = this.held();
            if (held != null) return this.encode(held.get(this.key, this.entry));
            SingleFlightLoader.Loaded<V> loaded = WatchHub.this.type.snapshots().loaded(this.id, this.keyId, this.entry);
            if (loaded == null) return UNLOADED;
            return this.encode(loaded.value());
        }

        @Override
        protected void load() {
            if (this.held() != null) return;
            WatchHub.this.type.snapshots().peekEntry(this.id, this.keyId, this.entry, () -> WatchHub.this.type.db().readEntry(this.ref, this.key, this.entry));
        }

        @Override
        protected void handOff() {
            HeldEntity<K> held = this.held();
            if (held == null) return;
            WatchHub.this.type.snapshots().seed(this.id, this.keyId, this.entry, () -> WatchHub.this.type.db().readEntry(this.ref, this.key, this.entry), held.get(this.key, this.entry));
        }

        // 条目不存在时是 null
        @Nullable
        private StoredValue encode(@Nullable V value) {
            return value == null ? null : this.key.valueForm().store(value);
        }
    }
}