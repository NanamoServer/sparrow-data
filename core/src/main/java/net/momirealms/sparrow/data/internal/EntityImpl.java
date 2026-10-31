package net.momirealms.sparrow.data.internal;

import net.momirealms.sparrow.data.entity.Entity;
import net.momirealms.sparrow.data.internal.key.AbstractDataKey;
import net.momirealms.sparrow.data.internal.key.DbKeyImpl;
import net.momirealms.sparrow.data.internal.key.DbMapCounterKeyImpl;
import net.momirealms.sparrow.data.internal.key.DbMapKeyImpl;
import net.momirealms.sparrow.data.internal.key.MapKeyImpl;
import net.momirealms.sparrow.data.internal.key.ValueKeyImpl;
import net.momirealms.sparrow.data.internal.memory.HeldEntity;
import net.momirealms.sparrow.data.key.DataKey;
import net.momirealms.sparrow.data.key.DbCounterKey;
import net.momirealms.sparrow.data.key.DbKey;
import net.momirealms.sparrow.data.key.DbMapCounterKey;
import net.momirealms.sparrow.data.key.DbMapKey;
import net.momirealms.sparrow.data.key.MapKey;
import net.momirealms.sparrow.data.key.MemoryDataKey;
import net.momirealms.sparrow.data.key.MemoryKey;
import net.momirealms.sparrow.data.key.MemoryMapKey;
import net.momirealms.sparrow.data.key.ValueKey;
import net.momirealms.sparrow.data.storage.EntityRef;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;

@ApiStatus.Internal
public record EntityImpl<K>(@NotNull EntityTypeImpl<K> type, @NotNull K id) implements Entity<K> {

    // 实体 ID 编码后超过 64 字节时抛 IllegalArgumentException. 每个操作都在调用线程上先调用它, 异常直接抛给调用方
    @NotNull
    public EntityRef ref() {
        return this.type.ref(this.id);
    }

    @Override
    public boolean isHeld() {
        return this.type.held(this.id) != null;
    }

    @NotNull
    @Override
    public <T> T get(@NotNull MemoryKey<K, T> key) {
        return this.requireHeld().get((ValueKeyImpl<K, T>) key);
    }

    @Override
    public <T> void write(@NotNull MemoryKey<K, T> key, @NotNull T value) {
        this.requireHeld().write((ValueKeyImpl<K, T>) key, value);
    }

    @NotNull
    @Override
    public <T> T update(@NotNull MemoryKey<K, T> key, @NotNull UnaryOperator<T> operator) {
        return this.requireHeld().update((ValueKeyImpl<K, T>) key, operator);
    }

    @Nullable
    @Override
    public <E, V> V get(@NotNull MemoryMapKey<K, E, V> key, @NotNull E entry) {
        MapKeyImpl<K, E, V> impl = (MapKeyImpl<K, E, V>) key;
        return this.requireHeld().get(impl, impl.entryCodec().encode(entry));
    }

    @NotNull
    @Override
    public <E, V> Map<E, V> getAll(@NotNull MemoryMapKey<K, E, V> key) {
        return this.requireHeld().getAll((MapKeyImpl<K, E, V>) key);
    }

    @Override
    public <E, V> void put(@NotNull MemoryMapKey<K, E, V> key, @NotNull E entry, @NotNull V value) {
        MapKeyImpl<K, E, V> impl = (MapKeyImpl<K, E, V>) key;
        this.requireHeld().put(impl, impl.entryCodec().encode(entry), value);
    }

    @Override
    public <E, V> void remove(@NotNull MemoryMapKey<K, E, V> key, @NotNull E entry) {
        MapKeyImpl<K, E, V> impl = (MapKeyImpl<K, E, V>) key;
        this.requireHeld().remove(impl, impl.entryCodec().encode(entry));
    }

    @Nullable
    @Override
    public <E, V> V compute(@NotNull MemoryMapKey<K, E, V> key, @NotNull E entry, @NotNull UnaryOperator<@Nullable V> operator) {
        MapKeyImpl<K, E, V> impl = (MapKeyImpl<K, E, V>) key;
        return this.requireHeld().compute(impl, impl.entryCodec().encode(entry), operator);
    }

    // 把这个键的当前值发给别的服, 不落库, 调用方不等结果. 要多久发一次由调用方决定
    @Override
    public void publish(@NotNull MemoryDataKey<K> key) {
        this.requireHeld().publish((AbstractDataKey<K>) key);
    }

    // 排进这个实体的落库链, future 在这次存储调用完成后结束. 写完会广播写到的键的失效, 别的服重读库
    @NotNull
    @Override
    public CompletableFuture<Void> save() {
        return this.requireHeld().save();
    }

    // 任何服, 不管这个实体被谁持有都能调用. future 在写库完成后结束, 广播在后台继续
    @NotNull
    @Override
    public <T> CompletableFuture<Void> override(@NotNull MemoryKey<K, T> key, @NotNull T value) {
        ValueKeyImpl<K, T> impl = (ValueKeyImpl<K, T>) key;
        return this.type.overrides().override(this.ref(), impl.storageKey(), null, impl.form(), value);
    }

    // 重置是删掉存储的值, 持有者把它读成默认值或 Optional.empty()
    @NotNull
    @Override
    public CompletableFuture<Void> reset(@NotNull MemoryKey<K, ?> key) {
        ValueKeyImpl<K, ?> impl = (ValueKeyImpl<K, ?>) key;
        return this.type.overrides().override(this.ref(), impl.storageKey(), null, impl.form(), null);
    }

    @NotNull
    @Override
    public <E, V> CompletableFuture<Void> override(@NotNull MemoryMapKey<K, E, V> key, @NotNull E entry, @NotNull V value) {
        MapKeyImpl<K, E, V> impl = (MapKeyImpl<K, E, V>) key;
        return this.type.overrides().override(this.ref(), impl.storageKey(), impl.entryCodec().encode(entry), impl.valueForm(), value);
    }

    // 重置映射键的条目就是删掉这个条目
    @NotNull
    @Override
    public <E> CompletableFuture<Void> reset(@NotNull MemoryMapKey<K, E, ?> key, @NotNull E entry) {
        MapKeyImpl<K, E, ?> impl = (MapKeyImpl<K, E, ?>) key;
        return this.type.overrides().override(this.ref(), impl.storageKey(), impl.entryCodec().encode(entry), impl.valueForm(), null);
    }

    // 内存键在本服持有时直接读容器, 其他情况读快照. 读取函数在调用之前就编码好实体 ID, 交给快照后不会再抛异常
    @Nullable
    @Override
    public <T> T peek(@NotNull ValueKey<K, T> key) {
        ValueKeyImpl<K, T> impl = (ValueKeyImpl<K, T>) key;
        HeldEntity<K> held = this.heldFor(key);
        if (held != null) return held.get(impl);
        EntityRef ref = this.ref();
        return this.type.snapshots().peek(this.id, impl.storageKey().id(), () -> this.type.db().read(ref, impl));
    }

    @Nullable
    @Override
    public <E, V> V peek(@NotNull MapKey<K, E, V> key, @NotNull E entry) {
        MapKeyImpl<K, E, V> impl = (MapKeyImpl<K, E, V>) key;
        String encoded = impl.entryCodec().encode(entry);
        HeldEntity<K> held = this.heldFor(key);
        if (held != null) return held.get(impl, encoded);
        EntityRef ref = this.ref();
        return this.type.snapshots().peekEntry(this.id, impl.storageKey().id(), encoded, () -> this.type.db().readEntry(ref, impl, encoded));
    }

    // 本服持有时内存键的条目总是已加载
    @Override
    public <E> boolean isLoaded(@NotNull MapKey<K, E, ?> key, @NotNull E entry) {
        if (this.heldFor(key) != null) return true;
        MapKeyImpl<K, E, ?> impl = (MapKeyImpl<K, E, ?>) key;
        return this.type.snapshots().isEntryLoaded(this.id, impl.storageKey().id(), impl.entryCodec().encode(entry));
    }

    @Nullable
    @Override
    public <E, V> Map<E, V> peekAll(@NotNull MapKey<K, E, V> key) {
        MapKeyImpl<K, E, V> impl = (MapKeyImpl<K, E, V>) key;
        HeldEntity<K> held = this.heldFor(key);
        if (held != null) return held.getAll(impl);
        EntityRef ref = this.ref();
        return this.type.snapshots().peek(this.id, impl.storageKey().id(), () -> this.type.db().readAll(ref, impl));
    }

    @NotNull
    @Override
    public <T> CompletableFuture<T> read(@NotNull ValueKey<K, T> key) {
        ValueKeyImpl<K, T> impl = (ValueKeyImpl<K, T>) key;
        HeldEntity<K> held = this.heldFor(key);
        if (held != null) return CompletableFuture.completedFuture(held.get(impl));
        return this.type.db().read(this.ref(), impl);
    }

    @NotNull
    @Override
    public <E, V> CompletableFuture<@Nullable V> read(@NotNull MapKey<K, E, V> key, @NotNull E entry) {
        MapKeyImpl<K, E, V> impl = (MapKeyImpl<K, E, V>) key;
        String encoded = impl.entryCodec().encode(entry);
        HeldEntity<K> held = this.heldFor(key);
        if (held != null) return CompletableFuture.completedFuture(held.get(impl, encoded));
        return this.type.db().readEntry(this.ref(), impl, encoded);
    }

    @NotNull
    @Override
    public <E, V> CompletableFuture<Map<E, V>> readAll(@NotNull MapKey<K, E, V> key) {
        MapKeyImpl<K, E, V> impl = (MapKeyImpl<K, E, V>) key;
        HeldEntity<K> held = this.heldFor(key);
        if (held != null) return CompletableFuture.completedFuture(held.getAll(impl));
        return this.type.db().readAll(this.ref(), impl);
    }

    @NotNull
    @Override
    public <T> CompletableFuture<Void> set(@NotNull DbKey<K, T> key, @NotNull T value) {
        return this.type.db().setValue(this.id, (DbKeyImpl<K, T>) key, value);
    }

    @NotNull
    @Override
    public <T> CompletableFuture<T> update(@NotNull DbKey<K, T> key, @NotNull UnaryOperator<T> operator) {
        return this.type.db().updateValue(this.id, (DbKeyImpl<K, T>) key, operator);
    }

    @NotNull
    @Override
    public CompletableFuture<Void> set(@NotNull DbCounterKey<K> key, long value) {
        return this.type.db().setCount(this.id, ((ValueKeyImpl<K, Long>) key).storageKey(), null, value);
    }

    @NotNull
    @Override
    public CompletableFuture<Long> add(@NotNull DbCounterKey<K> key, long delta) {
        return this.type.db().add(this.id, ((ValueKeyImpl<K, Long>) key).storageKey(), null, delta);
    }

    @NotNull
    @Override
    public CompletableFuture<OptionalLong> take(@NotNull DbCounterKey<K> key, long amount) {
        return this.type.db().take(this.id, ((ValueKeyImpl<K, Long>) key).storageKey(), null, amount);
    }

    @NotNull
    @Override
    public <E, V> CompletableFuture<Void> put(@NotNull DbMapKey<K, E, V> key, @NotNull E entry, @NotNull V value) {
        DbMapKeyImpl<K, E, V> impl = (DbMapKeyImpl<K, E, V>) key;
        return this.type.db().put(this.id, impl, impl.entryCodec().encode(entry), value);
    }

    @NotNull
    @Override
    public <E, V> CompletableFuture<Boolean> putIfAbsent(@NotNull DbMapKey<K, E, V> key, @NotNull E entry, @NotNull V value) {
        DbMapKeyImpl<K, E, V> impl = (DbMapKeyImpl<K, E, V>) key;
        return this.type.db().putIfAbsent(this.id, impl, impl.entryCodec().encode(entry), value);
    }

    @NotNull
    @Override
    public <E, V> CompletableFuture<@Nullable V> update(@NotNull DbMapKey<K, E, V> key, @NotNull E entry, @NotNull UnaryOperator<V> operator) {
        DbMapKeyImpl<K, E, V> impl = (DbMapKeyImpl<K, E, V>) key;
        return this.type.db().updateEntry(this.id, impl, impl.entryCodec().encode(entry), operator);
    }

    @NotNull
    @Override
    public <E> CompletableFuture<Boolean> remove(@NotNull DbMapKey<K, E, ?> key, @NotNull E entry) {
        MapKeyImpl<K, E, ?> impl = (MapKeyImpl<K, E, ?>) key;
        return this.type.db().remove(this.id, impl, impl.entryCodec().encode(entry));
    }

    @NotNull
    @Override
    public <E> CompletableFuture<Void> set(@NotNull DbMapCounterKey<K, E> key, @NotNull E entry, long value) {
        DbMapCounterKeyImpl<K, E> impl = (DbMapCounterKeyImpl<K, E>) key;
        return this.type.db().setCount(this.id, impl.storageKey(), impl.entryCodec().encode(entry), value);
    }

    @NotNull
    @Override
    public <E> CompletableFuture<Long> add(@NotNull DbMapCounterKey<K, E> key, @NotNull E entry, long delta) {
        DbMapCounterKeyImpl<K, E> impl = (DbMapCounterKeyImpl<K, E>) key;
        return this.type.db().add(this.id, impl.storageKey(), impl.entryCodec().encode(entry), delta);
    }

    @NotNull
    @Override
    public <E> CompletableFuture<OptionalLong> take(@NotNull DbMapCounterKey<K, E> key, @NotNull E entry, long amount) {
        DbMapCounterKeyImpl<K, E> impl = (DbMapCounterKeyImpl<K, E>) key;
        return this.type.db().take(this.id, impl.storageKey(), impl.entryCodec().encode(entry), amount);
    }

    @NotNull
    @Override
    public <E> CompletableFuture<Boolean> remove(@NotNull DbMapCounterKey<K, E> key, @NotNull E entry) {
        DbMapCounterKeyImpl<K, E> impl = (DbMapCounterKeyImpl<K, E>) key;
        return this.type.db().remove(this.id, impl, impl.entryCodec().encode(entry));
    }

    // 删除这个实体的全部数据. 在 Executor 上删库, 删完广播这个实体全部键的失效, future 在删库完成后结束.
    // 本服持有时抛 IllegalStateException, 持有方下次落库会把数据写回去; 别的服不持有由调用方保证
    @NotNull
    @Override
    public CompletableFuture<Void> delete() {
        if (this.type.held(this.id) != null) throw new IllegalStateException("Entity is held by this server: " + this.type.name() + " " + this.id);
        EntityRef ref = this.ref();
        DataServiceImpl service = this.type.service();
        return CompletableFuture.runAsync(() -> service.storage().deleteEntity(ref), service.executor())
                .thenRun(() -> service.messenger().invalidate(this.type.name(), ref.entityId(), null, null));
    }

    // 内存键的读写只有本服持有这个实体时才能用
    @NotNull
    private HeldEntity<K> requireHeld() {
        HeldEntity<K> held = this.type.held(this.id);
        if (held == null) throw new IllegalStateException("Entity is not held by this server: " + this.type.name() + " " + this.id);
        return held;
    }

    // 内存键在本服持有时返回它的容器, 其他情况返回 null
    @Nullable
    private HeldEntity<K> heldFor(@NotNull DataKey<K> key) {
        return key instanceof MemoryDataKey<?> ? this.type.held(this.id) : null;
    }
}