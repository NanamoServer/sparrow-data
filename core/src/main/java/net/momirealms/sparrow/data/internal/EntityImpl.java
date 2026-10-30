package net.momirealms.sparrow.data.internal;

import net.momirealms.sparrow.data.entity.Entity;
import net.momirealms.sparrow.data.internal.key.DbKeyImpl;
import net.momirealms.sparrow.data.internal.key.DbMapCounterKeyImpl;
import net.momirealms.sparrow.data.internal.key.DbMapKeyImpl;
import net.momirealms.sparrow.data.internal.key.MapKeyImpl;
import net.momirealms.sparrow.data.internal.key.ValueKeyImpl;
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
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @NotNull
    @Override
    public <T> T get(@NotNull MemoryKey<K, T> key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public <T> void write(@NotNull MemoryKey<K, T> key, @NotNull T value) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public <T> void writeAndPublish(@NotNull MemoryKey<K, T> key, @NotNull T value) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @NotNull
    @Override
    public <T> T update(@NotNull MemoryKey<K, T> key, @NotNull UnaryOperator<T> operator) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Nullable
    @Override
    public <E, V> V get(@NotNull MemoryMapKey<K, E, V> key, @NotNull E entry) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @NotNull
    @Override
    public <E, V> Map<E, V> getAll(@NotNull MemoryMapKey<K, E, V> key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public <E, V> void put(@NotNull MemoryMapKey<K, E, V> key, @NotNull E entry, @NotNull V value) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public <E, V> void remove(@NotNull MemoryMapKey<K, E, V> key, @NotNull E entry) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Nullable
    @Override
    public <E, V> V compute(@NotNull MemoryMapKey<K, E, V> key, @NotNull E entry, @NotNull UnaryOperator<@Nullable V> operator) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void publish(@NotNull MemoryDataKey<K> key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @NotNull
    @Override
    public CompletableFuture<Void> save() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @NotNull
    @Override
    public CompletableFuture<Void> saveAndPublish() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @NotNull
    @Override
    public <T> CompletableFuture<Void> override(@NotNull MemoryKey<K, T> key, @NotNull T value) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @NotNull
    @Override
    public CompletableFuture<Void> reset(@NotNull MemoryKey<K, ?> key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @NotNull
    @Override
    public <E, V> CompletableFuture<Void> override(@NotNull MemoryMapKey<K, E, V> key, @NotNull E entry, @NotNull V value) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @NotNull
    @Override
    public <E> CompletableFuture<Void> reset(@NotNull MemoryMapKey<K, E, ?> key, @NotNull E entry) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    // 读取函数在调用之前就编码好实体 ID, 交给快照后不会再抛异常
    @Nullable
    @Override
    public <T> T peek(@NotNull ValueKey<K, T> key) {
        requireDb(key);
        ValueKeyImpl<K, T> impl = (ValueKeyImpl<K, T>) key;
        EntityRef ref = this.ref();
        return this.type.snapshots().peek(this.id, impl.storageKey().id(), () -> this.type.db().read(ref, impl));
    }

    @Nullable
    @Override
    public <E, V> V peek(@NotNull MapKey<K, E, V> key, @NotNull E entry) {
        requireDb(key);
        MapKeyImpl<K, E, V> impl = (MapKeyImpl<K, E, V>) key;
        EntityRef ref = this.ref();
        String encoded = impl.entryCodec().encode(entry);
        return this.type.snapshots().peekEntry(this.id, impl.storageKey().id(), encoded, () -> this.type.db().readEntry(ref, impl, encoded));
    }

    @Override
    public <E> boolean isLoaded(@NotNull MapKey<K, E, ?> key, @NotNull E entry) {
        MapKeyImpl<K, E, ?> impl = (MapKeyImpl<K, E, ?>) key;
        return this.type.snapshots().isEntryLoaded(this.id, impl.storageKey().id(), impl.entryCodec().encode(entry));
    }

    @Nullable
    @Override
    public <E, V> Map<E, V> peekAll(@NotNull MapKey<K, E, V> key) {
        requireDb(key);
        MapKeyImpl<K, E, V> impl = (MapKeyImpl<K, E, V>) key;
        EntityRef ref = this.ref();
        return this.type.snapshots().peek(this.id, impl.storageKey().id(), () -> this.type.db().readAll(ref, impl));
    }

    @NotNull
    @Override
    public <T> CompletableFuture<T> read(@NotNull ValueKey<K, T> key) {
        requireDb(key);
        return this.type.db().read(this.ref(), (ValueKeyImpl<K, T>) key);
    }

    @NotNull
    @Override
    public <E, V> CompletableFuture<@Nullable V> read(@NotNull MapKey<K, E, V> key, @NotNull E entry) {
        requireDb(key);
        MapKeyImpl<K, E, V> impl = (MapKeyImpl<K, E, V>) key;
        return this.type.db().readEntry(this.ref(), impl, impl.entryCodec().encode(entry));
    }

    @NotNull
    @Override
    public <E, V> CompletableFuture<Map<E, V>> readAll(@NotNull MapKey<K, E, V> key) {
        requireDb(key);
        return this.type.db().readAll(this.ref(), (MapKeyImpl<K, E, V>) key);
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

    @NotNull
    @Override
    public CompletableFuture<Void> delete() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    private static void requireDb(@NotNull DataKey<?> key) {
        if (key instanceof MemoryDataKey<?>) throw new UnsupportedOperationException("Not implemented yet");
    }
}