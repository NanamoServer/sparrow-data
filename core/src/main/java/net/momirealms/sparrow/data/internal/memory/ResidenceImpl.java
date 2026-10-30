package net.momirealms.sparrow.data.internal.memory;

import net.momirealms.sparrow.data.entity.Residence;
import net.momirealms.sparrow.data.internal.DataServiceImpl;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.internal.key.AbstractDataKey;
import net.momirealms.sparrow.data.storage.EntityRef;
import net.momirealms.sparrow.data.storage.StorageKey;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

    // 读出全部内存键, 删掉上一个持有者可能留下的副本, 然后本服成为持有者并开始定时落库. 实体 ID 在调用线程上编码
    @NotNull
    @Override
    public CompletableFuture<Void> online(@NotNull K id) {
        EntityRef ref = this.type.ref(id);
        return this.enqueue(id, () -> this.load(id, ref));
    }

    // 先释放, 之后的修改都会抛异常; 再把释放之前的修改落库, 这次落库排在已经在途的落库后面
    @NotNull
    @Override
    public CompletableFuture<Void> offline(@NotNull K id) {
        return this.enqueue(id, () -> this.unload(id));
    }

    @NotNull
    private CompletableFuture<Void> load(@NotNull K id, @NotNull EntityRef ref) {
        // 加载, 建容器和删副本用同一份键集合
        List<AbstractDataKey<K>> keys = this.type.keys().memoryKeys();
        return CompletableFuture.supplyAsync(() -> new HeldEntity<>(ref, keys, this.service.storage().load(ref, storageKeys(keys))), this.service.executor())
                .thenCompose(held -> this.deleteReplicas(ref, keys).thenApply(ignored -> held))
                .thenAccept(held -> {
                    this.type.putHeld(id, held);
                    this.service.autoSaver().start(held, () -> this.autoSave(id, held));
                });
    }

    @NotNull
    private CompletableFuture<Void> unload(@NotNull K id) {
        HeldEntity<K> held = this.type.removeHeld(id);
        // online 失败时没有容器, 不用做什么
        if (held == null) return CompletableFuture.completedFuture(null);
        held.release();
        this.service.autoSaver().stop(held);
        return held.save(this.service.storage(), this.service.executor());
    }

    // 上一个持有者宕机时, 副本里可能是从没落库的值, 和刚读出的 checkpoint 对不上. 删掉之后观察方会改读存储.
    // 每个键一条 DEL, 同一条连接上的命令连着发出去, 只对确实删掉了副本的键广播失效
    @NotNull
    private CompletableFuture<Void> deleteReplicas(@NotNull EntityRef ref, @NotNull List<AbstractDataKey<K>> keys) {
        int size = keys.size();
        CompletableFuture<?>[] deletes = new CompletableFuture<?>[size];
        for (int i = 0; i < size; i++) {
            String keyId = keys.get(i).storageKey().id();
            deletes[i] = this.service.replicas().delete(ref, keyId).thenAccept(deleted -> {
                if (deleted) {
                    this.service.messenger().invalidate(this.type.name(), ref.entityId(), keyId, null);
                }
            });
        }
        return CompletableFuture.allOf(deletes);
    }

    // 定时落库没有调用方, 失败在这里记日志
    private void autoSave(@NotNull K id, @NotNull HeldEntity<K> held) {
        held.save(this.service.storage(), this.service.executor()).whenComplete((ignored, error) -> {
            if (error != null) {
                this.service.logger().error("Failed to auto save " + this.type.name() + " " + id, error);
            }
        });
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