package net.momirealms.sparrow.data.internal.roster;

import net.momirealms.sparrow.data.entity.Watch;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.internal.key.RosterKeyImpl;
import net.momirealms.sparrow.data.internal.sync.RosterChange;
import net.momirealms.sparrow.data.internal.util.Values;
import net.momirealms.sparrow.data.key.ValueCodec;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

@ApiStatus.Internal
public final class Roster<K, V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Roster.class);

    private final RosterService service;
    private final EntityTypeImpl<K> type;
    private final String keyId;
    private final ValueCodec<V> codec;
    private final byte[] redisKey;

    private final Map<K, Entry<V>> committed = new HashMap<>();   // 严格按 Redis 里的消息顺序推进的名单
    private final Map<K, Optimistic<V>> optimistic = new HashMap<>(); // 本服还没收到回显的操作, 盖在 committed 上面
    private final Map<Long, K> operations = new HashMap<>();      // 在途的本服操作: 序号 -> 实体
    private final ConcurrentHashMap<K, Entry<V>> visible = new ConcurrentHashMap<>(); // 查询读的就是它, 读的时候不加锁
    private final CopyOnWriteArrayList<Binding> watches = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<K, CopyOnWriteArrayList<Binding>> memberWatches = new ConcurrentHashMap<>();
    private @Nullable List<RosterChange> loading; // 装载期间暂存的消息, 没在装载时为 null
    private CompletableFuture<Void> loaded = CompletableFuture.completedFuture(null);

    // 一个名单在本服的镜像. committed 只由订阅到的消息推进, 所有服务器收到的顺序相同, 最终一致由它保证.
    // 本服的 announce 和 withdraw 先记进 optimistic, 让本服的查询立刻看到; 收到自己那条回显后撤掉, 之后看到的就是 committed.
    // 所有修改都在实例锁里, 回调在锁外
    Roster(@NotNull RosterService service, @NotNull RosterKeyImpl<K, V> key) {
        this.service = service;
        this.type = key.type();
        this.keyId = key.storageKey().id();
        this.codec = key.codec();
        this.redisKey = service.redisKey(this.type.name(), this.keyId);
    }

    @Nullable
    public V member(@NotNull K id) {
        Entry<V> entry = this.visible.get(id);
        return entry != null && this.service.alive(entry.serverId()) ? entry.value() : null;
    }

    @NotNull
    public Map<K, V> members() {
        Map<K, V> members = HashMap.newHashMap(this.visible.size());
        for (Map.Entry<K, Entry<V>> member : this.visible.entrySet()) {
            Entry<V> entry = member.getValue();
            if (this.service.alive(entry.serverId())) {
                members.put(member.getKey(), entry.value());
            }
        }
        return Collections.unmodifiableMap(members);
    }

    // 先行写进本服镜像再提交脚本, 两步在同一把锁里, 同一个实体的连续操作在 Redis 里的顺序和本服先行应用的顺序一致.
    // 脚本失败时不会有回显, 在这里撤掉这次先行的修改
    @NotNull
    public CompletableFuture<Void> announce(@NotNull K id, @NotNull V value) {
        byte[] field = this.type.ref(id).entityId();
        byte[] encoded = this.codec.encode(value);
        long sequence = this.service.nextSequence();
        boolean changed;
        CompletableFuture<Long> written;
        synchronized (this) {
            this.begin(id, sequence, new Entry<>(this.service.serverId(), value));
            changed = this.refresh(id);
            written = this.service.announce(this.redisKey, field, encoded, this.change(RosterChange.Kind.SET, sequence, field, encoded));
        }
        if (changed) {
            this.notifyWatches(List.of(id));
        }
        return written
                .whenComplete((ignored, error) -> {
                    if (error != null) this.settle(sequence);
                })
                .thenApply(ignored -> null);
    }

    // 镜像里这条记录归本服时才先行移除. 脚本以 Redis 里的归属为准, 没有删掉任何东西时不发消息, 也就没有回显, 由脚本的返回值收尾
    @NotNull
    public CompletableFuture<Void> withdraw(@NotNull K id) {
        byte[] field = this.type.ref(id).entityId();
        long sequence = this.service.nextSequence();
        boolean changed = false;
        CompletableFuture<Long> written;
        synchronized (this) {
            Entry<V> current = this.visible.get(id);
            if (current != null && current.serverId().equals(this.service.serverId())) {
                this.begin(id, sequence, null);
                changed = this.refresh(id);
            }
            written = this.service.withdraw(this.redisKey, field, this.change(RosterChange.Kind.REMOVE, sequence, field, null));
        }
        if (changed) {
            this.notifyWatches(List.of(id));
        }
        return written
                .whenComplete((removed, error) -> {
                    if (error != null || removed == 0L) this.settle(sequence);
                })
                .thenApply(ignored -> null);
    }

    // 在 Redis 的订阅线程上按到达顺序调用
    void receive(@NotNull RosterChange change) {
        List<K> changed = new ArrayList<>();
        synchronized (this) {
            if (this.loading != null) {
                this.loading.add(change);
                return;
            }
            List<K> affected = new ArrayList<>();
            this.apply(change, affected);
            int size = affected.size();
            for (int i = 0; i < size; i++) {
                K id = affected.get(i);
                if (this.refresh(id)) {
                    changed.add(id);
                }
            }
        }
        this.notifyWatches(changed);
    }

    // 注册时调用. 清掉本服上次运行留下的记录, 再装载镜像
    @NotNull
    synchronized CompletableFuture<Void> open() {
        return this.enqueue(false);
    }

    // 订阅恢复之后调用. 把本服名下的记录补回 Redis, 再重新装载镜像
    @NotNull
    synchronized CompletableFuture<Void> resync() {
        return this.enqueue(true);
    }

    // 注销键和数据层关闭时调用, 删掉 Redis 里本服名下的全部记录并通知其他服务器
    @NotNull
    CompletableFuture<Long> clearOwned() {
        return this.service.reset(this.redisKey, this.change(RosterChange.Kind.RESET, -1, null, null));
    }

    // 同一个名单的装载一次只做一轮, 后一轮等前一轮结束, 前一轮失败也照样开始
    @NotNull
    private CompletableFuture<Void> enqueue(boolean restore) {
        this.loaded = this.loaded
                .handle((ignored, error) -> (Void) null)
                .thenCompose(ignored -> this.reload(restore));
        return this.loaded;
    }

    // 先开始暂存消息, 再读全量, 读取期间的变化靠回放暂存的消息补上.
    // 这之前提交的本服操作已经排在读取前面, 读到的全量包含它们的结果, 所以读完时把它们一起收尾, 不再等回显
    @NotNull
    private CompletableFuture<Void> reload(boolean restore) {
        List<Long> settled;
        CompletableFuture<Map<byte[], byte[]>> snapshot;
        synchronized (this) {
            this.loading = new ArrayList<>();
            settled = new ArrayList<>(this.operations.keySet());
            if (restore) {
                this.restoreOwned();
            } else {
                this.service.reset(this.redisKey, this.change(RosterChange.Kind.RESET, -1, null, null));
            }
            snapshot = this.service.readAll(this.redisKey);
        }
        return snapshot
                .whenComplete((fields, error) -> {
                    if (error != null) this.abandonLoading();
                })
                .thenAccept(fields -> this.install(fields, settled));
    }

    // Redis 里缺了的本服记录补回去, 已经有记录的不动, 那可能是别的服务器在断线期间接管的. 调用方持有实例锁
    private void restoreOwned() {
        String self = this.service.serverId();
        for (Map.Entry<K, Entry<V>> member : this.committed.entrySet()) {
            Entry<V> entry = member.getValue();
            if (!entry.serverId().equals(self)) continue;

            byte[] field = this.type.idCodec().encode(member.getKey());
            byte[] encoded = this.codec.encode(entry.value());
            this.service.restore(this.redisKey, field, encoded, this.change(RosterChange.Kind.SET, -1, field, encoded));
        }
    }

    private void install(@NotNull Map<byte[], byte[]> fields, @NotNull List<Long> settled) {
        List<K> changed = new ArrayList<>();
        synchronized (this) {
            Set<K> affected = new HashSet<>(this.committed.keySet());
            this.committed.clear();
            for (Map.Entry<byte[], byte[]> field : fields.entrySet()) {
                K id = this.type.idCodec().decode(field.getKey());
                byte[] record = field.getValue();
                this.committed.put(id, new Entry<>(RosterService.owner(record), this.codec.decode(RosterService.value(record))));
                affected.add(id);
            }
            int settledCount = settled.size();
            for (int i = 0; i < settledCount; i++) {
                K id = this.operations.remove(settled.get(i));
                if (id != null) {
                    this.release(id);
                    affected.add(id);
                }
            }
            List<RosterChange> buffered = this.loading;
            this.loading = null;
            int bufferedCount = buffered.size();
            for (int i = 0; i < bufferedCount; i++) {
                this.apply(buffered.get(i), affected);
            }
            for (K id : affected) {
                if (this.refresh(id)) {
                    changed.add(id);
                }
            }
        }
        this.notifyWatches(changed);
    }

    // 读全量失败时结束这一轮装载. 暂存的消息接着应用到原来的名单上, 之后的消息照常处理
    private void abandonLoading() {
        List<K> changed = new ArrayList<>();
        synchronized (this) {
            List<RosterChange> buffered = this.loading;
            this.loading = null;
            Set<K> affected = new HashSet<>();
            int size = buffered.size();
            for (int i = 0; i < size; i++) {
                this.apply(buffered.get(i), affected);
            }
            for (K id : affected) {
                if (this.refresh(id)) {
                    changed.add(id);
                }
            }
        }
        this.notifyWatches(changed);
    }

    // 把一条消息应用到 committed. 是本服某次操作的回显时, 顺便撤掉那次操作先行的修改. 调用方持有实例锁
    private void apply(@NotNull RosterChange change, @NotNull Collection<K> affected) {
        switch (change.kind()) {
            case SET -> {
                K id = this.type.idCodec().decode(change.entityId());
                this.committed.put(id, new Entry<>(change.serverId(), this.codec.decode(change.value())));
                affected.add(id);
            }
            case REMOVE -> {
                K id = this.type.idCodec().decode(change.entityId());
                this.committed.remove(id);
                affected.add(id);
            }
            case RESET -> {
                Iterator<Map.Entry<K, Entry<V>>> iterator = this.committed.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<K, Entry<V>> member = iterator.next();
                    if (!member.getValue().serverId().equals(change.serverId())) continue;

                    iterator.remove();
                    affected.add(member.getKey());
                }
            }
        }
        if (change.sequence() >= 0 && change.serverId().equals(this.service.serverId())) {
            K id = this.operations.remove(change.sequence());
            if (id != null) {
                this.release(id);
                affected.add(id);
            }
        }
    }

    // 一次不会有回显的本服操作在这里收尾: 脚本失败了, 或者撤回时发现记录不归本服
    private void settle(long sequence) {
        boolean changed;
        K id;
        synchronized (this) {
            id = this.operations.remove(sequence);
            if (id == null) return;

            this.release(id);
            changed = this.refresh(id);
        }
        if (changed) {
            this.notifyWatches(List.of(id));
        }
    }

    // entry 为 null 表示先行移除. 调用方持有实例锁
    private void begin(@NotNull K id, long sequence, @Nullable Entry<V> entry) {
        Optimistic<V> pending = this.optimistic.computeIfAbsent(id, ignored -> new Optimistic<>());
        pending.operations++;
        pending.entry = entry;
        this.operations.put(sequence, id);
    }

    // 这个实体在途的本服操作都结束后, 撤掉先行的修改. 调用方持有实例锁
    private void release(@NotNull K id) {
        Optimistic<V> pending = this.optimistic.get(id);
        if (--pending.operations == 0) {
            this.optimistic.remove(id);
        }
    }

    // 重新算出这个实体对查询可见的记录, 返回可见的值有没有变. 归属换了而值没变时不算变化. 调用方持有实例锁
    private boolean refresh(@NotNull K id) {
        Optimistic<V> pending = this.optimistic.get(id);
        Entry<V> next = pending != null ? pending.entry : this.committed.get(id);
        Entry<V> previous = next == null ? this.visible.remove(id) : this.visible.put(id, next);
        if (previous == null || next == null) return previous != next;
        return !Values.same(previous.value(), next.value());
    }

    @NotNull
    private RosterChange change(@NotNull RosterChange.Kind kind, long sequence, byte @Nullable [] entityId, byte @Nullable [] value) {
        return new RosterChange(this.type.name(), this.keyId, kind, this.service.serverId(), sequence, entityId, value);
    }

    @NotNull
    public Watch watch(@NotNull Runnable onChange) {
        Binding binding = new Binding(null, onChange);
        this.watches.add(binding);
        return binding;
    }

    @NotNull
    public Watch watch(@NotNull K id, @NotNull Runnable onChange) {
        Binding binding = new Binding(id, onChange);
        // 加进列表和关闭时摘掉空列表都在 compute 里, 不会加进一个刚被摘掉的列表
        this.memberWatches.compute(id, (ignored, bindings) -> {
            CopyOnWriteArrayList<Binding> watching = bindings == null ? new CopyOnWriteArrayList<>() : bindings;
            watching.add(binding);
            return watching;
        });
        return binding;
    }

    // 关掉全部 watch. 注销键时每个再通知最后一次, 数据层关闭时不通知
    void closeWatches(boolean notifyLast) {
        List<Binding> closing = new ArrayList<>(this.watches);
        for (CopyOnWriteArrayList<Binding> bindings : this.memberWatches.values()) {
            closing.addAll(bindings);
        }
        this.watches.clear();
        this.memberWatches.clear();
        int size = closing.size();
        for (int i = 0; i < size; i++) {
            Binding binding = closing.get(i);
            if (binding.closed.compareAndSet(false, true) && notifyLast) {
                binding.notifyChange();
            }
        }
    }

    // 在锁外调用. 整个名单的 watch 一批变化只通知一次
    private void notifyWatches(@NotNull List<K> changed) {
        if (changed.isEmpty()) return;

        for (Binding binding : this.watches) {
            if (!binding.closed.get()) binding.notifyChange();
        }
        if (this.memberWatches.isEmpty()) return;

        int size = changed.size();
        for (int i = 0; i < size; i++) {
            CopyOnWriteArrayList<Binding> bindings = this.memberWatches.get(changed.get(i));
            if (bindings == null) continue;

            for (Binding binding : bindings) {
                if (!binding.closed.get()) binding.notifyChange();
            }
        }
    }

    // 名单里的一条记录和它的所属服务器
    private record Entry<V>(@NotNull String serverId, @NotNull V value) {
    }

    // 一个实体上还没收到回显的本服操作. entry 是最后一次操作先行写进去的记录, null 表示先行移除
    private static final class Optimistic<V> {
        private int operations;
        private @Nullable Entry<V> entry;
    }

    private final class Binding implements Watch {
        private final @Nullable K id; // null 表示观察整个名单
        private final Runnable onChange;
        private final AtomicBoolean closed = new AtomicBoolean();
        private final CompletableFuture<Void> ready = CompletableFuture.completedFuture(null);

        private Binding(@Nullable K id, @NotNull Runnable onChange) {
            this.id = id;
            this.onChange = onChange;
        }

        @NotNull
        @Override
        public CompletableFuture<Void> ready() {
            return this.ready;
        }

        @Override
        public void close() {
            if (!this.closed.compareAndSet(false, true)) return;

            if (this.id == null) {
                Roster.this.watches.remove(this);
                return;
            }
            Roster.this.memberWatches.computeIfPresent(this.id, (ignored, bindings) -> {
                bindings.remove(this);
                return bindings.isEmpty() ? null : bindings;
            });
        }

        // 回调来自宿主, 它的异常在这里记日志, 不影响其他 watch 和触发改动的调用方
        private void notifyChange() {
            try {
                this.onChange.run();
            } catch (RuntimeException exception) {
                LOGGER.error("Failed to notify a roster watch on {} {}", Roster.this.type.name(), Roster.this.keyId, exception);
            }
        }
    }
}