package net.momirealms.sparrow.data.internal.memory;

import net.momirealms.sparrow.data.entity.Residence;
import net.momirealms.sparrow.data.internal.DataServiceImpl;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.internal.key.AbstractDataKey;
import net.momirealms.sparrow.data.storage.EntityRef;
import net.momirealms.sparrow.data.storage.StorageKey;
import net.momirealms.sparrow.data.storage.StoredEntity;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

@ApiStatus.Internal
public final class ResidenceImpl<K> implements Residence<K> {
    private final EntityTypeImpl<K> type;
    private final DataServiceImpl service;
    private final Map<K, CompletableFuture<Void>> pending = new ConcurrentHashMap<>();

    // 一个实体类型的持有权入口. 驻留方不会并发调用同一个实体的 online 和 offline, 但不一定等上一个 future 完成,
    // 所以同一个实体的 online 和 offline 在这里按调用顺序排队, 后一个等前一个结束再开始
    public ResidenceImpl(@NotNull EntityTypeImpl<K> type, @NotNull DataServiceImpl service) {
        this.type = type;
        this.service = service;
    }

    // 读出全部内存键, 然后本服成为持有者并开始定时落库. 实体 ID 在调用线程上编码
    @NotNull
    @Override
    public CompletableFuture<Void> online(@NotNull K id) {
        EntityRef ref = this.type.ref(id);
        return this.enqueue(id, () -> this.load(id, ref));
    }

    // 先释放, 之后的修改和 publish 都会抛异常; 再把释放之前的修改落库, 这次落库排在已经在途的落库后面
    @NotNull
    @Override
    public CompletableFuture<Void> offline(@NotNull K id) {
        return this.enqueue(id, () -> this.unload(id));
    }

    // DataService.close 时调用. 持有中的实体和还有 online / offline 在排队的实体各做一次 offline, 排在它们已有的步骤后面,
    // 正在 online 的实体加载完之后马上被释放
    @NotNull
    public CompletableFuture<Void> offlineAll() {
        Set<K> ids = new HashSet<>(this.type.heldEntities().keySet());
        ids.addAll(this.pending.keySet());
        CompletableFuture<?>[] offlines = new CompletableFuture<?>[ids.size()];
        int index = 0;
        for (K id : ids) {
            offlines[index++] = this.offline(id);
        }
        return CompletableFuture.allOf(offlines);
    }

    // 读库之前开始缓冲收到的覆盖, 放进持有表之前先应用它们; 加载失败时丢掉缓冲
    @NotNull
    private CompletableFuture<Void> load(@NotNull K id, @NotNull EntityRef ref) {
        OverrideService<K> overrides = this.type.overrides();
        overrides.beginLoading(id);
        return this.loadSlots(ref, this.type.keys().memoryKeys())
                .thenCompose(slots -> this.hold(id, new HeldEntity<>(id, ref, slots, this.service)))
                .whenComplete((ignored, error) -> {
                    if (error != null) {
                        overrides.abortLoading(id);
                    }
                });
    }

    // 加载期间键表可能变了. 放进持有表之前和键表对一遍, 新注册的内存键再读一次补进容器, 直到一致才放进持有表并开始定时落库
    @NotNull
    private CompletableFuture<Void> hold(@NotNull K id, @NotNull HeldEntity<K> held) {
        List<AbstractDataKey<K>> missing = this.type.overrides().finishLoading(id, held);
        if (missing.isEmpty()) {
            this.type.watches().touchEntity(id);
            held.startAutoSave();
            return CompletableFuture.completedFuture(null);
        }
        return this.loadSlots(held.ref(), missing).thenCompose(slots -> {
            held.attach(slots);
            return this.hold(id, held);
        });
    }

    // 在 Executor 上读库并解码成槽. online 和运行中注册内存键时的补读都用它
    @NotNull
    CompletableFuture<HeldEntity.Slots<K>> loadSlots(@NotNull EntityRef ref, @NotNull List<AbstractDataKey<K>> keys) {
        return CompletableFuture.supplyAsync(
                () -> {
                    // 没有内存键的类型不读库, 它的存储可能还没准备
                    StoredEntity stored = keys.isEmpty() ? new StoredEntity(Map.of(), Map.of()) : this.service.storage().load(ref, storageKeys(keys));
                    return HeldEntity.decode(keys, stored);
                },
                this.service.executor()
        );
    }

    // 先释放容器, 再把被观察的内存键的值交给快照, 最后移出持有表, 观察方看到的值不会断档. 落库完成之前这个实体内存键的快照不重读,
    // 期间收到的失效等落库后一起处理, 读不到落库之前的旧值
    @NotNull
    private CompletableFuture<Void> unload(@NotNull K id) {
        HeldEntity<K> held = this.type.held(id);
        // online 失败时没有容器, 不用做什么
        if (held == null) return CompletableFuture.completedFuture(null);
        List<AbstractDataKey<K>> memoryKeys = this.type.keys().memoryKeys();
        Set<String> keyIds = HashSet.newHashSet(memoryKeys.size());
        for (int i = 0, size = memoryKeys.size(); i < size; i++) {
            keyIds.add(memoryKeys.get(i).storageKey().id());
        }
        this.type.snapshots().fence(id, keyIds);
        held.release();
        this.type.watches().handOff(id);
        this.type.removeHeld(id);
        this.type.watches().touchEntity(id);
        return held.save().whenComplete((ignored, error) -> this.type.snapshots().unfence(id));
    }

    // 新的一步接在这个实体的上一步后面, 上一步不管成功还是失败, 结束了就轮到它. 队列走空时从 pending 里移除
    @NotNull
    private CompletableFuture<Void> enqueue(@NotNull K id, @NotNull Supplier<CompletableFuture<Void>> step) {
        CompletableFuture<Void> turn = new CompletableFuture<>();
        CompletableFuture<Void> done = turn.thenCompose(ignored -> step.get());
        CompletableFuture<Void> previous = this.pending.put(id, done);
        done.whenComplete((ignored, error) -> this.pending.remove(id, done));
        if (previous == null) {
            turn.complete(null);
        } else {
            previous.whenComplete((ignored, error) -> turn.complete(null));
        }
        return done;
    }

    @NotNull
    private static List<StorageKey> storageKeys(@NotNull List<? extends AbstractDataKey<?>> keys) {
        int size = keys.size();
        List<StorageKey> storageKeys = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            storageKeys.add(keys.get(i).storageKey());
        }
        return storageKeys;
    }
}