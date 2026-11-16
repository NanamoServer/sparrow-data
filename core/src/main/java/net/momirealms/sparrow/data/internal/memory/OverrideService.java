package net.momirealms.sparrow.data.internal.memory;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.DataServiceImpl;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.internal.key.AbstractDataKey;
import net.momirealms.sparrow.data.internal.key.ValueForm;
import net.momirealms.sparrow.data.storage.EntityRef;
import net.momirealms.sparrow.data.storage.EntityStorage;
import net.momirealms.sparrow.data.storage.StorageKey;
import net.momirealms.sparrow.data.storage.StoredValue;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@ApiStatus.Internal
public final class OverrideService<K> {
    private final EntityTypeImpl<K> type;
    private final DataServiceImpl service;

    private final Object lock = new Object();
    private final Map<K, List<Received<K>>> loading = new HashMap<>();
    private final Map<AbstractDataKey<K>, Map<K, List<Received<K>>>> backfilling = new HashMap<>();

    // 一个实体类型的外部覆盖. 发起端任何服都能调用; 持有端收到消息后把新值写进容器, 正在 online 加载或正在补读的先缓冲, 读完按收到的顺序应用.
    // 内存键表的变化, 加载完成时放进持有表, 补读完成时加槽, 以及覆盖的缓冲都在 lock 里, 它们互相看得到对方的结果, 收到的覆盖不会落在两步之间
    public OverrideService(@NotNull EntityTypeImpl<K> type, @NotNull DataServiceImpl service) {
        this.type = type;
        this.service = service;
    }

    // 发起端. 在 Executor 上写库, 写完广播覆盖消息, future 在写库完成后结束.
    // value 为 null 是重置, 删掉存储的值
    @NotNull
    public <T> CompletableFuture<Void> override(@NotNull EntityRef ref, @NotNull StorageKey key, @NotNull ValueForm<T> form, @Nullable T value) {
        EntityStorage storage = this.service.storage();
        return CompletableFuture.supplyAsync(() -> {
            StoredValue stored = value == null ? null : form.store(value);
            if (stored == null) {
                storage.remove(ref, key);
            } else {
                storage.set(ref, key, stored);
            }
            return stored;
        }, this.service.executor()).thenAccept(stored -> this.service.messenger().override(ref.entityType(), ref.entityId(), key.id(), stored));
    }

    // 持有端, 在 Redis 的订阅线程上直接应用, 订阅线程只有一个, 覆盖按收到的顺序应用.
    // 本服没注册这个键, 或者既没持有也没在加载这个实体时什么都不做, 快照的重读已经由失效处理了
    public void receive(@NotNull K id, @NotNull String keyId, @Nullable StoredValue stored) {
        HeldEntity<K> held;
        Received<K> received;
        synchronized (this.lock) {
            AbstractDataKey<K> key = this.type.keys().get(Key.of(keyId));
            if (key == null) return;
            received = new Received<>(key, stored);
            List<Received<K>> buffer = this.loading.get(id);
            if (buffer != null) {
                buffer.add(received);
                return;
            }
            held = this.type.held(id);
            if (held == null) return;
            // 持有中的实体还没有这个键的槽, 说明这个键正在补读
            if (!held.has(key)) {
                Map<K, List<Received<K>>> pending = this.backfilling.get(key);
                if (pending != null) {
                    pending.computeIfAbsent(id, ignored -> new ArrayList<>()).add(received);
                }
                return;
            }
        }
        held.override(received.key(), received.stored());
        this.type.watches().touch(id, keyId);
    }

    // online 开始读库之前调用. 读库可能发生在某次覆盖写库之前, 读出来的是旧值, 所以从这里开始收到的覆盖先缓冲
    public void beginLoading(@NotNull K id) {
        synchronized (this.lock) {
            this.loading.put(id, new ArrayList<>());
        }
    }

    // 加载完成时调用. 加载期间又注册了新的内存键时, 返回这些键, 调用方读完补进容器后再调用一次.
    // 键表一致时, 去掉加载期间注销的键, 按收到的顺序应用缓冲的覆盖, 再放进持有表, 返回空列表; 之后收到的覆盖直接写进容器.
    // 缓冲里可能有读库之前就已经写进库的覆盖, 再应用一次写的是同样的值
    @NotNull
    public List<AbstractDataKey<K>> finishLoading(@NotNull K id, @NotNull HeldEntity<K> held) {
        synchronized (this.lock) {
            List<AbstractDataKey<K>> registered = this.type.keys().memoryKeys();
            List<AbstractDataKey<K>> missing = new ArrayList<>();
            int size = registered.size();
            for (int i = 0; i < size; i++) {
                AbstractDataKey<K> key = registered.get(i);
                if (!held.has(key)) {
                    missing.add(key);
                }
            }
            if (!missing.isEmpty()) return missing;
            held.retain(registered);
            applyAll(held, this.loading.remove(id));
            this.type.putHeld(id, held);
            return List.of();
        }
    }

    // 加载失败时调用. 缓冲的覆盖都已经写进库了, 直接丢掉
    public void abortLoading(@NotNull K id) {
        synchronized (this.lock) {
            this.loading.remove(id);
        }
    }

    // 运行中注册内存键时调用. 进键表, 开始缓冲这个键的覆盖, 取出要补读的持有中的实体. 正在加载的实体不在里面,
    // 它们放进持有表之前会在 finishLoading 里自己补上这个键
    @NotNull
    public Map<K, HeldEntity<K>> beginBackfill(@NotNull AbstractDataKey<K> key) {
        synchronized (this.lock) {
            this.type.keys().register(key);
            this.backfilling.put(key, new HashMap<>());
            return this.type.heldEntities();
        }
    }

    // 一个实体补读完成时调用. 加上槽, 再按收到的顺序应用补读期间缓冲的覆盖; 补读期间 offline 的容器不再加槽
    public void finishBackfill(@NotNull K id, @NotNull HeldEntity<K> held, @NotNull AbstractDataKey<K> key, @NotNull HeldEntity.Slots<K> slots) {
        synchronized (this.lock) {
            held.attach(slots);
            List<Received<K>> buffer = this.backfilling.get(key).remove(id);
            if (buffer == null) return;
            applyAll(held, buffer);
        }
    }

    // 全部补读结束时调用, 不管成功还是失败
    public void endBackfill(@NotNull AbstractDataKey<K> key) {
        synchronized (this.lock) {
            this.backfilling.remove(key);
        }
    }

    // 注销内存键时调用. 移出键表, 取出持有中的实体; 正在加载的实体放进持有表之前会在 finishLoading 里自己去掉这个键
    @NotNull
    public Map<K, HeldEntity<K>> beginUnregister(@NotNull AbstractDataKey<K> key) {
        synchronized (this.lock) {
            this.type.keys().unregister(key);
            return this.type.heldEntities();
        }
    }

    private static <K> void applyAll(@NotNull HeldEntity<K> held, @NotNull List<Received<K>> buffer) {
        int size = buffer.size();
        for (int i = 0; i < size; i++) {
            Received<K> received = buffer.get(i);
            held.override(received.key(), received.stored());
        }
    }

    private record Received<K>(@NotNull AbstractDataKey<K> key, @Nullable StoredValue stored) {
    }
}