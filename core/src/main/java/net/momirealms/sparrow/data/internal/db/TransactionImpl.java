package net.momirealms.sparrow.data.internal.db;

import net.momirealms.sparrow.data.Transaction;
import net.momirealms.sparrow.data.entity.Entity;
import net.momirealms.sparrow.data.internal.EntityImpl;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.internal.key.DbKeyImpl;
import net.momirealms.sparrow.data.internal.key.ValueKeyImpl;
import net.momirealms.sparrow.data.key.DbCounterKey;
import net.momirealms.sparrow.data.key.DbKey;
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
            this.changed(target, impl.storageKey(), value);
        }
    }

    @NotNull
    @Override
    public <K, T> T update(@NotNull Entity<K> entity, @NotNull DbKey<K, T> key, @NotNull UnaryOperator<T> operator) {
        EntityImpl<K> target = (EntityImpl<K>) entity;
        DbKeyImpl<K, T> impl = (DbKeyImpl<K, T>) key;
        DbOperations.Update<T> update = DbOperations.updateValue(this.attempt, target.ref(), impl, operator);
        if (update.changed()) {
            this.changed(target, impl.storageKey(), update.value());
        }
        return update.value();
    }

    @Override
    public <K> void set(@NotNull Entity<K> entity, @NotNull DbCounterKey<K> key, long value) {
        EntityImpl<K> target = (EntityImpl<K>) entity;
        StorageKey storageKey = ((ValueKeyImpl<K, Long>) key).storageKey();
        this.attempt.set(target.ref(), storageKey, new StoredValue.OfLong(value));
        this.changed(target, storageKey, value);
    }

    @Override
    public <K> long add(@NotNull Entity<K> entity, @NotNull DbCounterKey<K> key, long delta) {
        EntityImpl<K> target = (EntityImpl<K>) entity;
        StorageKey storageKey = ((ValueKeyImpl<K, Long>) key).storageKey();
        long value = this.attempt.add(target.ref(), storageKey, delta);
        this.changed(target, storageKey, value);
        return value;
    }

    @NotNull
    @Override
    public <K> OptionalLong take(@NotNull Entity<K> entity, @NotNull DbCounterKey<K> key, long amount) {
        EntityImpl<K> target = (EntityImpl<K>) entity;
        StorageKey storageKey = ((ValueKeyImpl<K, Long>) key).storageKey();
        OptionalLong balance = this.attempt.take(target.ref(), storageKey, amount);
        if (balance.isPresent()) {
            this.changed(target, storageKey, balance.getAsLong());
        }
        return balance;
    }

    private <K> void changed(@NotNull EntityImpl<K> entity, @NotNull StorageKey key, @Nullable Object value) {
        this.changes.put(new Change<>(entity.type(), entity.id(), key.id()), value);
    }

    // 事务里改过的一个对象. 同一个对象改了多次时只记最后写进去的值, 快照只更新一次, 失效也只广播一次
    private record Change<K>(@NotNull EntityTypeImpl<K> type, @NotNull K id, @NotNull String keyId) {

        void written(@Nullable Object value) {
            this.type.snapshots().put(this.id, this.keyId, value);
            this.type.db().invalidate(this.type.ref(this.id).entityId(), this.keyId);
        }
    }
}