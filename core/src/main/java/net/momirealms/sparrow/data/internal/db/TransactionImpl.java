package net.momirealms.sparrow.data.internal.db;

import net.momirealms.sparrow.data.Transaction;
import net.momirealms.sparrow.data.entity.Entity;
import net.momirealms.sparrow.data.internal.EntityImpl;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.internal.key.DbKeyImpl;
import net.momirealms.sparrow.data.internal.key.DbMapCounterKeyImpl;
import net.momirealms.sparrow.data.internal.key.DbMapKeyImpl;
import net.momirealms.sparrow.data.internal.key.MapKeyImpl;
import net.momirealms.sparrow.data.internal.key.ValueKeyImpl;
import net.momirealms.sparrow.data.key.DbCounterKey;
import net.momirealms.sparrow.data.key.DbKey;
import net.momirealms.sparrow.data.key.DbMapCounterKey;
import net.momirealms.sparrow.data.key.DbMapKey;
import net.momirealms.sparrow.data.storage.EntityStorage;
import net.momirealms.sparrow.data.storage.StorageKey;
import net.momirealms.sparrow.data.storage.StoredValue;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.function.Function;
import java.util.function.UnaryOperator;

@ApiStatus.Internal
public final class TransactionImpl implements Transaction {
    private final EntityStorage storage;

    private final Map<Change<?>, Object> changes = new LinkedHashMap<>(); // 改过的对象 -> 最后写进去的值
    private EntityStorage attempt;
    private boolean rollback;

    public TransactionImpl(@NotNull EntityStorage storage) {
        this.storage = storage;
    }

    // 在存储的事务里执行 body. 死锁时存储会回滚并重跑 body, 所以每一轮都换上这一轮的连接, 改动记录和回滚标记从头开始;
    // 提交之后按最后一轮的改动, 先用每个对象最后写进去的值更新本服快照, 再广播失效, 别的服重读时一定能读到提交的值. 回滚了快照和广播都不动.
    // 返回的是最后一轮 body 的返回值, 回滚时也一样
    @Nullable
    public <R> R run(@NotNull Function<Transaction, R> body) {
        List<R> result = new ArrayList<>(1);
        boolean committed = this.storage.transaction(attempt -> {
            this.attempt = attempt;
            this.changes.clear();
            this.rollback = false;
            result.clear();
            result.add(body.apply(this));
            return !this.rollback;
        });
        if (committed) {
            this.changes.forEach(Change::written);
        }
        return result.getFirst();
    }

    @Override
    public void rollback() {
        this.rollback = true;
    }

    @Override
    public <K, T> void set(@NotNull Entity<K> entity, @NotNull DbKey<K, T> key, @NotNull T value) {
        EntityImpl<K> target = (EntityImpl<K>) entity;
        DbKeyImpl<K, T> impl = (DbKeyImpl<K, T>) key;
        if (DbOperations.setValue(this.attempt, target.ref(), impl, value)) {
            this.changed(target, impl.storageKey(), null, value);
        }
    }

    @NotNull
    @Override
    public <K, T> T update(@NotNull Entity<K> entity, @NotNull DbKey<K, T> key, @NotNull UnaryOperator<T> operator) {
        EntityImpl<K> target = (EntityImpl<K>) entity;
        DbKeyImpl<K, T> impl = (DbKeyImpl<K, T>) key;
        DbOperations.Update<T> update = DbOperations.updateValue(this.attempt, target.ref(), impl, operator);
        if (update.changed()) {
            this.changed(target, impl.storageKey(), null, update.value());
        }
        return update.value();
    }

    @Override
    public <K> void set(@NotNull Entity<K> entity, @NotNull DbCounterKey<K> key, long value) {
        this.setCount((EntityImpl<K>) entity, ((ValueKeyImpl<K, Long>) key).storageKey(), null, value);
    }

    @Override
    public <K> long add(@NotNull Entity<K> entity, @NotNull DbCounterKey<K> key, long delta) {
        return this.addCount((EntityImpl<K>) entity, ((ValueKeyImpl<K, Long>) key).storageKey(), null, delta);
    }

    @NotNull
    @Override
    public <K> OptionalLong take(@NotNull Entity<K> entity, @NotNull DbCounterKey<K> key, long amount) {
        return this.takeCount((EntityImpl<K>) entity, ((ValueKeyImpl<K, Long>) key).storageKey(), null, amount);
    }

    @Override
    public <K, E, V> void put(@NotNull Entity<K> entity, @NotNull DbMapKey<K, E, V> key, @NotNull E entry, @NotNull V value) {
        EntityImpl<K> target = (EntityImpl<K>) entity;
        DbMapKeyImpl<K, E, V> impl = (DbMapKeyImpl<K, E, V>) key;
        String encoded = impl.entryCodec().encode(entry);
        DbOperations.put(this.attempt, target.ref(), impl, encoded, value);
        this.changed(target, impl.storageKey(), encoded, value);
    }

    @Override
    public <K, E, V> boolean putIfAbsent(@NotNull Entity<K> entity, @NotNull DbMapKey<K, E, V> key, @NotNull E entry, @NotNull V value) {
        EntityImpl<K> target = (EntityImpl<K>) entity;
        DbMapKeyImpl<K, E, V> impl = (DbMapKeyImpl<K, E, V>) key;
        String encoded = impl.entryCodec().encode(entry);
        boolean inserted = DbOperations.putIfAbsent(this.attempt, target.ref(), impl, encoded, value);
        if (inserted) {
            this.changed(target, impl.storageKey(), encoded, value);
        }
        return inserted;
    }

    @Nullable
    @Override
    public <K, E, V> V update(@NotNull Entity<K> entity, @NotNull DbMapKey<K, E, V> key, @NotNull E entry, @NotNull UnaryOperator<V> operator) {
        EntityImpl<K> target = (EntityImpl<K>) entity;
        DbMapKeyImpl<K, E, V> impl = (DbMapKeyImpl<K, E, V>) key;
        String encoded = impl.entryCodec().encode(entry);
        DbOperations.Update<V> update = DbOperations.updateEntry(this.attempt, target.ref(), impl, encoded, operator);
        if (update == null) return null;
        if (update.changed()) {
            this.changed(target, impl.storageKey(), encoded, update.value());
        }
        return update.value();
    }

    @Override
    public <K, E> boolean remove(@NotNull Entity<K> entity, @NotNull DbMapKey<K, E, ?> key, @NotNull E entry) {
        return this.removeEntry((EntityImpl<K>) entity, (MapKeyImpl<K, E, ?>) key, entry);
    }

    @Override
    public <K, E> void set(@NotNull Entity<K> entity, @NotNull DbMapCounterKey<K, E> key, @NotNull E entry, long value) {
        DbMapCounterKeyImpl<K, E> impl = (DbMapCounterKeyImpl<K, E>) key;
        this.setCount((EntityImpl<K>) entity, impl.storageKey(), impl.entryCodec().encode(entry), value);
    }

    @Override
    public <K, E> long add(@NotNull Entity<K> entity, @NotNull DbMapCounterKey<K, E> key, @NotNull E entry, long delta) {
        DbMapCounterKeyImpl<K, E> impl = (DbMapCounterKeyImpl<K, E>) key;
        return this.addCount((EntityImpl<K>) entity, impl.storageKey(), impl.entryCodec().encode(entry), delta);
    }

    @NotNull
    @Override
    public <K, E> OptionalLong take(@NotNull Entity<K> entity, @NotNull DbMapCounterKey<K, E> key, @NotNull E entry, long amount) {
        DbMapCounterKeyImpl<K, E> impl = (DbMapCounterKeyImpl<K, E>) key;
        return this.takeCount((EntityImpl<K>) entity, impl.storageKey(), impl.entryCodec().encode(entry), amount);
    }

    @Override
    public <K, E> boolean remove(@NotNull Entity<K> entity, @NotNull DbMapCounterKey<K, E> key, @NotNull E entry) {
        return this.removeEntry((EntityImpl<K>) entity, (DbMapCounterKeyImpl<K, E>) key, entry);
    }

    private <K> void setCount(@NotNull EntityImpl<K> entity, @NotNull StorageKey key, @Nullable String entry, long value) {
        this.attempt.set(entity.ref(), key, entry, new StoredValue.OfLong(value));
        this.changed(entity, key, entry, value);
    }

    private <K> long addCount(@NotNull EntityImpl<K> entity, @NotNull StorageKey key, @Nullable String entry, long delta) {
        long value = this.attempt.add(entity.ref(), key, entry, delta);
        this.changed(entity, key, entry, value);
        return value;
    }

    @NotNull
    private <K> OptionalLong takeCount(@NotNull EntityImpl<K> entity, @NotNull StorageKey key, @Nullable String entry, long amount) {
        OptionalLong balance = this.attempt.take(entity.ref(), key, entry, amount);
        if (balance.isPresent()) {
            this.changed(entity, key, entry, balance.getAsLong());
        }
        return balance;
    }

    private <K, E> boolean removeEntry(@NotNull EntityImpl<K> entity, @NotNull MapKeyImpl<K, E, ?> key, @NotNull E entry) {
        String encoded = key.entryCodec().encode(entry);
        boolean removed = this.attempt.remove(entity.ref(), key.storageKey(), encoded);
        // 删掉的条目记成不存在时的值, 普通映射键是 null, 计数映射键是 0
        if (removed) {
            this.changed(entity, key.storageKey(), encoded, key.valueForm().load(null));
        }
        return removed;
    }

    private <K> void changed(@NotNull EntityImpl<K> entity, @NotNull StorageKey key, @Nullable String entry, @Nullable Object value) {
        this.changes.put(new Change<>(entity.type(), entity.id(), key.id(), entry), value);
    }

    // 事务里改过的一个对象. 同一个对象改了多次时只记最后写进去的值, 快照只更新一次, 失效也只广播一次
    private record Change<K>(@NotNull EntityTypeImpl<K> type, @NotNull K id, @NotNull String keyId, @Nullable String entry) {

        void written(@Nullable Object value) {
            this.type.snapshots().put(this.id, this.keyId, this.entry, value);
            this.type.db().invalidate(this.type.ref(this.id).entityId(), this.keyId, this.entry);
        }
    }
}