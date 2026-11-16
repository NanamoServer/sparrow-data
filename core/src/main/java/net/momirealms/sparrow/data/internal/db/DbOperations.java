package net.momirealms.sparrow.data.internal.db;

import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.internal.key.DbKeyImpl;
import net.momirealms.sparrow.data.internal.key.ValueForm;
import net.momirealms.sparrow.data.internal.key.ValueKeyImpl;
import net.momirealms.sparrow.data.internal.sync.DataMessenger;
import net.momirealms.sparrow.data.storage.EntityRef;
import net.momirealms.sparrow.data.storage.EntityStorage;
import net.momirealms.sparrow.data.storage.StorageKey;
import net.momirealms.sparrow.data.storage.StoredValue;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

@ApiStatus.Internal
public final class DbOperations<K> {
    private final EntityTypeImpl<K> type;
    private final EntityStorage storage;
    private final Executor executor;
    private final DataMessenger messenger;

    // 一个实体类型的 DB 权威读写. 实体 ID 在调用线程上编码, ID 超长时异常直接抛给调用方;
    // 存储在宿主的 Executor 上同步调用, 写完后先用写进去的值更新本服快照, 再广播失效
    public DbOperations(@NotNull EntityTypeImpl<K> type, @NotNull EntityStorage storage, @NotNull Executor executor, @NotNull DataMessenger messenger) {
        this.type = type;
        this.storage = storage;
        this.executor = executor;
        this.messenger = messenger;
    }

    @NotNull
    public <T> CompletableFuture<T> read(@NotNull EntityRef ref, @NotNull ValueKeyImpl<K, T> key) {
        return this.supply(() -> key.form().load(this.storage.read(ref, key.storageKey())));
    }

    @NotNull
    public <T> CompletableFuture<Void> setValue(@NotNull K id, @NotNull DbKeyImpl<K, T> key, @NotNull T value) {
        EntityRef ref = this.type.ref(id);
        return this.run(() -> {
            if (setValue(this.storage, ref, key, value)) {
                this.written(id, ref, key.storageKey(), value);
            }
        });
    }

    // 可选键写入 Optional.empty() 就是删除. 返回存储里的数据有没有变
    static <T> boolean setValue(@NotNull EntityStorage storage, @NotNull EntityRef ref, @NotNull DbKeyImpl<?, T> key, @NotNull T value) {
        StoredValue stored = key.form().store(value);
        if (stored == null) return storage.remove(ref, key.storageKey());
        storage.set(ref, key.storageKey(), stored);
        return true;
    }

    @NotNull
    public <T> CompletableFuture<T> updateValue(@NotNull K id, @NotNull DbKeyImpl<K, T> key, @NotNull UnaryOperator<T> operator) {
        EntityRef ref = this.type.ref(id);
        return this.supply(() -> {
            Update<T> update = updateValue(this.storage, ref, key, operator);
            this.updated(id, ref, key.storageKey(), update);
            return update.value();
        });
    }

    // CAS 循环, 条件是读到的原始字节. 写入失败说明读完之后有人改过, 重读后再执行一次 operator, 所以 operator 可能执行多次
    @NotNull
    static <T> Update<T> updateValue(@NotNull EntityStorage storage, @NotNull EntityRef ref, @NotNull DbKeyImpl<?, T> key, @NotNull UnaryOperator<T> operator) {
        ValueForm<T> form = key.form();
        StorageKey storageKey = key.storageKey();
        while (true) {
            StoredValue current = storage.read(ref, storageKey);
            byte[] expected = current == null ? null : ((StoredValue.OfBytes) current).value();
            T value = operator.apply(form.load(current));
            StoredValue stored = form.store(value);
            if (unchanged(expected, stored)) return new Update<>(value, false);
            if (compareAndWrite(storage, ref, storageKey, expected, stored)) return new Update<>(value, true);
        }
    }

    // 新字节和读到的一样时不用写, 相当于在读到的那一刻做了一次没有改动的更新. 可选键的 stored 为 null 时, 本来就没有值也算没有改动.
    // 存储因此不会收到 expected 和新值相同的 CAS
    private static boolean unchanged(byte @Nullable [] expected, @Nullable StoredValue stored) {
        if (stored == null) return expected == null;
        return Arrays.equals(expected, ((StoredValue.OfBytes) stored).value());
    }

    // 可选键的 operator 返回 Optional.empty() 时 stored 为 null, 这时以读到的字节为条件删除
    private static boolean compareAndWrite(@NotNull EntityStorage storage, @NotNull EntityRef ref, @NotNull StorageKey key, byte @Nullable [] expected, @Nullable StoredValue stored) {
        if (stored == null) return storage.compareAndRemove(ref, key, expected);
        return storage.compareAndSet(ref, key, expected, ((StoredValue.OfBytes) stored).value());
    }

    @NotNull
    public CompletableFuture<Void> setCount(@NotNull K id, @NotNull StorageKey key, long value) {
        EntityRef ref = this.type.ref(id);
        return this.run(() -> {
            this.storage.set(ref, key, new StoredValue.OfLong(value));
            this.written(id, ref, key, value);
        });
    }

    @NotNull
    public CompletableFuture<Long> add(@NotNull K id, @NotNull StorageKey key, long delta) {
        EntityRef ref = this.type.ref(id);
        return this.supply(() -> {
            long value = this.storage.add(ref, key, delta);
            this.written(id, ref, key, value);
            return value;
        });
    }

    // 余额不足时什么都没写, 不更新快照也不广播
    @NotNull
    public CompletableFuture<OptionalLong> take(@NotNull K id, @NotNull StorageKey key, long amount) {
        EntityRef ref = this.type.ref(id);
        return this.supply(() -> {
            OptionalLong balance = this.storage.take(ref, key, amount);
            if (balance.isPresent()) {
                this.written(id, ref, key, balance.getAsLong());
            }
            return balance;
        });
    }

    // update 没有改动库里的数据时只用读到的值更新本服快照, 不广播. 别的服的快照要么已经是这个值, 要么正等着之前那次写入的失效
    private void updated(@NotNull K id, @NotNull EntityRef ref, @NotNull StorageKey key, @NotNull Update<?> update) {
        if (update.changed()) {
            this.written(id, ref, key, update.value());
        } else {
            this.type.snapshots().put(id, key.id(), update.value());
        }
    }

    // 写入方先用自己写进去的值更新快照, 再广播失效. 别的服靠这条失效重读; 本服自己也会收到, 再读一次就能追上并发写入的其他服
    private void written(@NotNull K id, @NotNull EntityRef ref, @NotNull StorageKey key, @Nullable Object value) {
        this.type.snapshots().put(id, key.id(), value);
        this.invalidate(ref.entityId(), key.id());
    }

    void invalidate(byte @NotNull [] entityId, @NotNull String keyId) {
        this.messenger.invalidate(this.type.name(), entityId, keyId);
    }

    @NotNull
    private <T> CompletableFuture<T> supply(@NotNull Supplier<T> action) {
        return CompletableFuture.supplyAsync(action, this.executor);
    }

    @NotNull
    private CompletableFuture<Void> run(@NotNull Runnable action) {
        return CompletableFuture.runAsync(action, this.executor);
    }

    // update 的结果. changed 为 false 表示新值编码后和读到的字节相同, 没有写库
    record Update<T>(T value, boolean changed) {
    }
}