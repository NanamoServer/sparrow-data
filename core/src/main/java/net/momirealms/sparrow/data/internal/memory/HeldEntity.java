package net.momirealms.sparrow.data.internal.memory;

import net.momirealms.sparrow.data.internal.DataServiceImpl;
import net.momirealms.sparrow.data.internal.key.AbstractDataKey;
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
import java.util.function.DoubleUnaryOperator;
import java.util.function.IntUnaryOperator;
import java.util.function.LongUnaryOperator;
import java.util.function.UnaryOperator;

@ApiStatus.Internal
public final class HeldEntity<K> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HeldEntity.class);

    private final K id;
    private final EntityRef ref;
    private final DataServiceImpl service;

    private final Map<AbstractDataKey<K>, HeldSlot<?>> values;

    private final AtomicReference<CompletableFuture<Void>> lastSave = new AtomicReference<>(CompletableFuture.completedFuture(null));
    private volatile boolean released;

    // 一个实体在本服持有期间的全部内存键值, 用这个对象的锁把所有修改, 落库取脏和 publish 串起来. 读值不加锁, 值都是不可变对象.
    // 运行中注册和注销内存键时, 槽也是在锁里增减, 所以槽表用 ConcurrentHashMap, 不加锁的读一样能看到
    public HeldEntity(@NotNull K id, @NotNull EntityRef ref, @NotNull Slots<K> slots, @NotNull DataServiceImpl service) {
        this.id = id;
        this.ref = ref;
        this.service = service;
        this.values = new ConcurrentHashMap<>(slots.values());
    }

    // 把读出来的存储值解码成槽, 在锁外调用. 没有存储值的键用默认值, 没有默认值的是 null
    @NotNull
    public static <K> Slots<K> decode(@NotNull List<AbstractDataKey<K>> keys, @NotNull StoredEntity stored) {
        int size = keys.size();
        Map<AbstractDataKey<K>, HeldSlot<?>> values = HashMap.newHashMap(size);
        for (int i = 0; i < size; i++) {
            AbstractDataKey<K> key = keys.get(i);
            values.put(key, loadValue((ValueKeyImpl<?, ?>) key, stored.values().get(key.storageKey().id())));
        }
        return new Slots<>(values);
    }

    public boolean has(@NotNull AbstractDataKey<K> key) {
        return this.values.containsKey(key);
    }

    // 补读或者 online 补上缺的键时调用. 已经释放的容器不再加槽
    public synchronized void attach(@NotNull Slots<K> slots) {
        if (this.released) return;
        this.values.putAll(slots.values());
    }

    // online 放进持有表之前调用, 去掉加载期间被注销的键. 刚加载的容器里这些槽没有脏数据
    public synchronized void retain(@NotNull Collection<AbstractDataKey<K>> keys) {
        this.values.keySet().retainAll(keys);
    }

    @Nullable
    public <T> T get(@NotNull ValueKeyImpl<K, T> key) {
        return this.value(key).value();
    }

    public synchronized <T> void write(@NotNull ValueKeyImpl<K, T> key, @Nullable T value) {
        this.requireHeld();
        this.value(key).set(value);
    }

    // operator 在锁里执行, 同一个实体上的其他修改和落库取脏都要等它. operator 返回 null 是删除存储的值, 这时返回的是默认值
    @Nullable
    public synchronized <T> T update(@NotNull ValueKeyImpl<K, T> key, @NotNull UnaryOperator<T> operator) {
        this.requireHeld();
        HeldSlot<T> slot = this.value(key);
        slot.set(operator.apply(slot.value()));
        return slot.value();
    }

    // int, long, double 键的原始值读写. 值在槽里是一个 long: int 和 long 就是数值本身, double 是它的位模式, 由调用方换算
    public long bits(@NotNull AbstractDataKey<K> key) {
        return this.number(key).bits();
    }

    public synchronized void setBits(@NotNull AbstractDataKey<K> key, long bits) {
        this.requireHeld();
        this.number(key).setBits(bits);
    }

    public synchronized int addInt(@NotNull AbstractDataKey<K> key, int delta) {
        this.requireHeld();
        HeldNumber<?> slot = this.number(key);
        int value = (int) slot.bits() + delta;
        slot.setBits(value);
        return value;
    }

    public synchronized long addLong(@NotNull AbstractDataKey<K> key, long delta) {
        this.requireHeld();
        HeldNumber<?> slot = this.number(key);
        long value = slot.bits() + delta;
        slot.setBits(value);
        return value;
    }

    public synchronized double addDouble(@NotNull AbstractDataKey<K> key, double delta) {
        this.requireHeld();
        HeldNumber<?> slot = this.number(key);
        double value = Double.longBitsToDouble(slot.bits()) + delta;
        slot.setBits(Double.doubleToRawLongBits(value));
        return value;
    }

    public synchronized int updateInt(@NotNull AbstractDataKey<K> key, @NotNull IntUnaryOperator operator) {
        this.requireHeld();
        HeldNumber<?> slot = this.number(key);
        int value = operator.applyAsInt((int) slot.bits());
        slot.setBits(value);
        return value;
    }

    public synchronized long updateLong(@NotNull AbstractDataKey<K> key, @NotNull LongUnaryOperator operator) {
        this.requireHeld();
        HeldNumber<?> slot = this.number(key);
        long value = operator.applyAsLong(slot.bits());
        slot.setBits(value);
        return value;
    }

    public synchronized double updateDouble(@NotNull AbstractDataKey<K> key, @NotNull DoubleUnaryOperator operator) {
        this.requireHeld();
        HeldNumber<?> slot = this.number(key);
        double value = operator.applyAsDouble(Double.longBitsToDouble(slot.bits()));
        slot.setBits(Double.doubleToRawLongBits(value));
        return value;
    }

    // 收到覆盖时调用. 新值写进容器并标脏, 下一次落库会再写一遍, 盖掉在途落库可能写回库里的旧值.
    // 已经释放的容器不再应用; 键已经注销, 槽也摘掉了时跳过
    public synchronized void override(@NotNull AbstractDataKey<K> key, @Nullable StoredValue stored) {
        if (this.released) return;
        HeldSlot<?> value = this.values.get(key);
        if (value != null) value.overwrite(stored);
    }

    // 广播这个键的当前值, 不落库, 观察方直接把值放进快照. 取值, 编码和发送都在锁里,
    // 同一个键的 publish 按调用顺序发出; 发送只是交给 broker 的连接, 不等 Redis 回复
    public synchronized void publish(@NotNull AbstractDataKey<K> key) {
        this.requireHeld();
        this.service.messenger().publishValue(this.ref.entityType(), this.ref.entityId(), key.storageKey().id(), this.values.get(key).stored());
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
        List<Dirty> taken = new ArrayList<>();
        synchronized (this) {
            for (HeldSlot<?> slot : this.values.values()) {
                slot.takeDirty(taken);
            }
        }
        this.store(taken);
    }

    private void saveDetached(@NotNull AbstractDataKey<K> key) {
        List<Dirty> taken = new ArrayList<>();
        synchronized (this) {
            HeldSlot<?> value = this.values.remove(key);
            if (value != null) value.takeDirty(taken);
        }
        this.store(taken);
    }

    // 在锁外编码取出的脏数据, 主线程的修改不用等编码. 落库之后广播写到的每个键的失效, 观察方重读库
    private void store(@NotNull List<Dirty> taken) {
        if (taken.isEmpty()) return;
        List<SaveBatch.Write> writes = new ArrayList<>();
        List<SaveBatch.Remove> removes = new ArrayList<>();
        int takenCount = taken.size();
        for (int i = 0; i < takenCount; i++) {
            taken.get(i).encode(writes, removes);
        }
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
            this.service.messenger().invalidate(this.ref.entityType(), this.ref.entityId(), keyId);
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
    private <T> HeldSlot<T> value(@NotNull ValueKeyImpl<K, T> key) {
        return (HeldSlot<T>) this.values.get(key);
    }

    @NotNull
    private HeldNumber<?> number(@NotNull AbstractDataKey<K> key) {
        return (HeldNumber<?>) this.values.get(key);
    }

    @NotNull
    private static <T> HeldSlot<T> loadValue(@NotNull ValueKeyImpl<?, T> key, @Nullable StoredValue stored) {
        ValueForm<T> form = key.form();
        if (form instanceof ValueForm.NumberForm<T> number) return new HeldNumber<>(key.storageKey(), number, number.loadBits(stored));
        return new HeldValue<>(key.storageKey(), form, form.load(stored));
    }

    // 一个槽在锁里取出的脏数据, 出锁以后再编码. 值都是不可变对象, 晚一点编码得到的字节一样
    @FunctionalInterface
    interface Dirty {

        void encode(@NotNull List<SaveBatch.Write> writes, @NotNull List<SaveBatch.Remove> removes);
    }

    // 解码好的槽, 还没放进容器
    public record Slots<K>(@NotNull Map<AbstractDataKey<K>, HeldSlot<?>> values) {
    }
}