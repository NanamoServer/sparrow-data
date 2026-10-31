package net.momirealms.sparrow.data.internal.memory;

import net.momirealms.sparrow.data.internal.DataServiceImpl;
import net.momirealms.sparrow.data.internal.key.AbstractDataKey;
import net.momirealms.sparrow.data.internal.key.MapKeyImpl;
import net.momirealms.sparrow.data.internal.key.ValueForm;
import net.momirealms.sparrow.data.internal.key.ValueKeyImpl;
import net.momirealms.sparrow.data.storage.EntityRef;
import net.momirealms.sparrow.data.storage.SaveBatch;
import net.momirealms.sparrow.data.storage.StoredEntity;
import net.momirealms.sparrow.data.storage.StoredValue;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

@ApiStatus.Internal
public final class HeldEntity<K> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HeldEntity.class);

    private final K id;
    private final EntityRef ref;
    private final DataServiceImpl service;

    private final Map<AbstractDataKey<K>, HeldValue<?>> values;
    private final Map<AbstractDataKey<K>, HeldEntries<?, ?>> entries;

    private final AtomicReference<CompletableFuture<Void>> lastSave = new AtomicReference<>(CompletableFuture.completedFuture(null));
    private volatile boolean released;

    // 一个实体在本服持有期间的全部内存键值, 用这个对象的锁把所有修改, 落库取脏和 publish 串起来. 读单个值和单个条目不加锁, 值都是不可变对象.
    // 运行中注册和注销内存键时, 槽也是在锁里增减, 所以槽表用 ConcurrentHashMap, 不加锁的读一样能看到
    public HeldEntity(@NotNull K id, @NotNull EntityRef ref, @NotNull Slots<K> slots, @NotNull DataServiceImpl service) {
        this.id = id;
        this.ref = ref;
        this.service = service;
        this.values = new ConcurrentHashMap<>(slots.values());
        this.entries = new ConcurrentHashMap<>(slots.entries());
    }

    // 把读出来的存储值解码成槽, 在锁外调用. 没有存储值的键, 有默认值的用默认值, 可选键用 Optional.empty()
    @NotNull
    public static <K> Slots<K> decode(@NotNull List<AbstractDataKey<K>> keys, @NotNull StoredEntity stored) {
        int size = keys.size();
        Map<AbstractDataKey<K>, HeldValue<?>> values = HashMap.newHashMap(size);
        Map<AbstractDataKey<K>, HeldEntries<?, ?>> entries = HashMap.newHashMap(size);
        for (int i = 0; i < size; i++) {
            AbstractDataKey<K> key = keys.get(i);
            String keyId = key.storageKey().id();
            if (key instanceof MapKeyImpl<?, ?, ?> map) {
                entries.put(key, loadEntries(map, stored.entries().getOrDefault(keyId, Map.of())));
            } else {
                values.put(key, loadValue((ValueKeyImpl<?, ?>) key, stored.values().get(keyId)));
            }
        }
        return new Slots<>(values, entries);
    }

    public boolean has(@NotNull AbstractDataKey<K> key) {
        return this.values.containsKey(key) || this.entries.containsKey(key);
    }

    // 补读或者 online 补上缺的键时调用. 已经释放的容器不再加槽
    public synchronized void attach(@NotNull Slots<K> slots) {
        if (this.released) return;
        this.values.putAll(slots.values());
        this.entries.putAll(slots.entries());
    }

    // online 放进持有表之前调用, 去掉加载期间被注销的键. 刚加载的容器里这些槽没有脏数据
    public synchronized void retain(@NotNull Collection<AbstractDataKey<K>> keys) {
        this.values.keySet().retainAll(keys);
        this.entries.keySet().retainAll(keys);
    }

    @NotNull
    public <T> T get(@NotNull ValueKeyImpl<K, T> key) {
        return this.value(key).value();
    }

    public synchronized <T> void write(@NotNull ValueKeyImpl<K, T> key, @NotNull T value) {
        this.requireHeld();
        this.value(key).set(value);
    }

    // operator 在锁里执行, 同一个实体上的其他修改和落库取脏都要等它
    @NotNull
    public synchronized <T> T update(@NotNull ValueKeyImpl<K, T> key, @NotNull UnaryOperator<T> operator) {
        this.requireHeld();
        HeldValue<T> slot = this.value(key);
        T value = operator.apply(slot.value());
        slot.set(value);
        return value;
    }

    @Nullable
    public <V> V get(@NotNull MapKeyImpl<K, ?, V> key, @NotNull String entry) {
        return this.entries(key).get(entry);
    }

    @NotNull
    public synchronized <E, V> Map<E, V> getAll(@NotNull MapKeyImpl<K, E, V> key) {
        return this.entries(key).getAll();
    }

    public synchronized <V> void put(@NotNull MapKeyImpl<K, ?, V> key, @NotNull String entry, @NotNull V value) {
        this.requireHeld();
        this.entries(key).put(entry, value);
    }

    public synchronized void remove(@NotNull MapKeyImpl<K, ?, ?> key, @NotNull String entry) {
        this.requireHeld();
        this.entries(key).remove(entry);
    }

    @Nullable
    public synchronized <V> V compute(@NotNull MapKeyImpl<K, ?, V> key, @NotNull String entry, @NotNull UnaryOperator<@Nullable V> operator) {
        this.requireHeld();
        return this.entries(key).compute(entry, operator);
    }

    // 收到覆盖时调用. 新值写进容器并标脏, 下一次落库会再写一遍, 盖掉在途落库可能写回库里的旧值.
    // 已经释放的容器不再应用; 键已经注销, 槽也摘掉了时跳过
    public synchronized void override(@NotNull AbstractDataKey<K> key, @Nullable String entry, @Nullable StoredValue stored) {
        if (this.released) return;
        HeldValue<?> value = this.values.get(key);
        HeldEntries<?, ?> entries = this.entries.get(key);
        if (value != null) value.overwrite(stored);
        if (entries != null) entries.overwrite(entry, stored);
    }

    // 广播这个键的当前值, 不落库, 观察方直接把值放进快照, 映射键发全部条目. 取值, 编码和发送都在锁里,
    // 同一个键的 publish 按调用顺序发出; 发送只是交给 broker 的连接, 不等 Redis 回复
    public synchronized void publish(@NotNull AbstractDataKey<K> key) {
        this.requireHeld();
        String keyId = key.storageKey().id();
        HeldValue<?> value = this.values.get(key);
        if (value != null) {
            this.service.messenger().publishValue(this.ref.entityType(), this.ref.entityId(), keyId, value.stored());
            return;
        }
        this.service.messenger().publishEntries(this.ref.entityType(), this.ref.entityId(), keyId, this.entries.get(key).storedEntries());
    }

    // 接在上一次落库后面, 轮到时在 Executor 上取脏再交给存储. 取脏发生在上一次存储调用结束之后, 所以写进库的顺序就是取脏的顺序
    @NotNull
    public CompletableFuture<Void> save() {
        return this.enqueueSave(this::saveDirty);
    }

    // 注销内存键时调用. 排进落库链, 轮到时在锁里摘掉这个键的槽并取出它的脏数据, 然后落库. 排在已经在途的落库后面,
    // 这个键最后写进库的就是摘槽那一刻的值
    @NotNull
    public CompletableFuture<Void> detach(@NotNull AbstractDataKey<K> key) {
        return this.enqueueSave(() -> this.saveDetached(key));
    }

    // online 放进持有表之后调用. 每隔 saveInterval 落库一次, 实体 online 的时间各不相同, 落库自然分散在整个间隔里.
    // 计时用 JDK 共享的 delayedExecutor, 到点在宿主的 Executor 上排一次落库, 再约下一次; 释放之后到点的那次直接结束
    public void startAutoSave() {
        CompletableFuture.delayedExecutor(this.service.saveInterval().toMillis(), TimeUnit.MILLISECONDS, this.service.executor()).execute(this::autoSave);
    }

    // offline 时调用. 之后的修改和 publish 都抛 IllegalStateException, 包括释放之前就拿到了这个容器的调用方; 释放之前的修改由随后的落库带走
    public synchronized void release() {
        this.released = true;
    }

    @NotNull
    public EntityRef ref() {
        return this.ref;
    }

    // 上一次落库不管成功还是失败, 结束了就轮到这一次, 它的异常已经交给了它自己的调用方
    @NotNull
    private CompletableFuture<Void> enqueueSave(@NotNull Runnable task) {
        CompletableFuture<Void> turn = new CompletableFuture<>();
        CompletableFuture<Void> saved = turn.thenRunAsync(task, this.service.executor());
        this.lastSave.getAndSet(saved).whenComplete((ignored, error) -> turn.complete(null));
        return saved;
    }

    private void saveDirty() {
        List<SaveBatch.Write> writes = new ArrayList<>();
        List<SaveBatch.Remove> removes = new ArrayList<>();
        synchronized (this) {
            for (HeldValue<?> slot : this.values.values()) {
                slot.takeDirty(writes, removes);
            }
            for (HeldEntries<?, ?> slot : this.entries.values()) {
                slot.takeDirty(writes, removes);
            }
        }
        this.store(writes, removes);
    }

    private void saveDetached(@NotNull AbstractDataKey<K> key) {
        List<SaveBatch.Write> writes = new ArrayList<>();
        List<SaveBatch.Remove> removes = new ArrayList<>();
        synchronized (this) {
            HeldValue<?> value = this.values.remove(key);
            HeldEntries<?, ?> entries = this.entries.remove(key);
            if (value != null) value.takeDirty(writes, removes);
            if (entries != null) entries.takeDirty(writes, removes);
        }
        this.store(writes, removes);
    }

    // 落库之后广播写到的每个键的失效, 映射键按整个键发, 观察方重读库
    private void store(@NotNull List<SaveBatch.Write> writes, @NotNull List<SaveBatch.Remove> removes) {
        if (writes.isEmpty() && removes.isEmpty()) return;
        this.service.storage().save(this.ref, new SaveBatch(writes, removes));
        Set<String> keyIds = new HashSet<>();
        int writeCount = writes.size();
        for (int i = 0; i < writeCount; i++) {
            keyIds.add(writes.get(i).key().id());
        }
        int removeCount = removes.size();
        for (int i = 0; i < removeCount; i++) {
            keyIds.add(removes.get(i).key().id());
        }
        for (String keyId : keyIds) {
            this.service.messenger().invalidate(this.ref.entityType(), this.ref.entityId(), keyId, null);
        }
    }

    // 定时落库没有调用方, 失败在这里记日志
    private void autoSave() {
        if (this.released) return;
        this.save().whenComplete((ignored, error) -> {
            if (error != null) {
                LOGGER.error("Failed to auto save {} {}", this.ref.entityType(), this.id, error);
            }
        });
        this.startAutoSave();
    }

    private void requireHeld() {
        if (this.released) throw new IllegalStateException("Entity is no longer held by this server: " + this.ref.entityType() + " " + this.id);
    }

    @SuppressWarnings("unchecked")
    @NotNull
    private <T> HeldValue<T> value(@NotNull ValueKeyImpl<K, T> key) {
        return (HeldValue<T>) this.values.get(key);
    }

    @SuppressWarnings("unchecked")
    @NotNull
    private <E, V> HeldEntries<E, V> entries(@NotNull MapKeyImpl<K, E, V> key) {
        return (HeldEntries<E, V>) this.entries.get(key);
    }

    @NotNull
    private static <T> HeldValue<T> loadValue(@NotNull ValueKeyImpl<?, T> key, @Nullable StoredValue stored) {
        ValueForm<T> form = key.form();
        return new HeldValue<>(key.storageKey(), form, form.load(stored));
    }

    @NotNull
    private static <E, V> HeldEntries<E, V> loadEntries(@NotNull MapKeyImpl<?, E, V> key, @NotNull Map<String, StoredValue> stored) {
        ValueForm<V> form = key.valueForm();
        Map<String, V> entries = HashMap.newHashMap(stored.size());
        stored.forEach((entry, value) -> entries.put(entry, form.load(value)));
        return new HeldEntries<>(key.storageKey(), key.entryCodec(), form, entries);
    }

    // 解码好的槽, 还没放进容器
    public record Slots<K>(@NotNull Map<AbstractDataKey<K>, HeldValue<?>> values, @NotNull Map<AbstractDataKey<K>, HeldEntries<?, ?>> entries) {
    }
}