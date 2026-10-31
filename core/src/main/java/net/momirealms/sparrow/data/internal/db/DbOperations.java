package net.momirealms.sparrow.data.internal.db;

import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.internal.key.DbKeyImpl;
import net.momirealms.sparrow.data.internal.key.DbMapKeyImpl;
import net.momirealms.sparrow.data.internal.key.MapKeyImpl;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
    private final DataMessenger<?> messenger;

    // 一个实体类型的 DB 权威读写. 实体 ID 在调用线程上编码, ID 超长时异常直接抛给调用方;
    // 存储在宿主的 Executor 上同步调用, 写完后先用写进去的值更新本服快照, 再广播失效
    public DbOperations(@NotNull EntityTypeImpl<K> type, @NotNull EntityStorage storage, @NotNull Executor executor, @NotNull DataMessenger<?> messenger) {
        this.type = type;
        this.storage = storage;
        this.executor = executor;
        this.messenger = messenger;
    }

    @NotNull
    public <T> CompletableFuture<T> read(@NotNull EntityRef ref, @NotNull ValueKeyImpl<K, T> key) {
        return this.supply(() -> key.form().load(this.storage.read(ref, key.storageKey(), null)));
    }

    // 普通映射键的条目不存在时是 null, 计数映射键是 0
    @NotNull
    public <V> CompletableFuture<@Nullable V> readEntry(@NotNull EntityRef ref, @NotNull MapKeyImpl<K, ?, V> key, @NotNull String entry) {
        return this.supply(() -> key.valueForm().load(this.storage.read(ref, key.storageKey(), entry)));
    }

    @NotNull
    public <E, V> CompletableFuture<Map<E, V>> readAll(@NotNull EntityRef ref, @NotNull MapKeyImpl<K, E, V> key) {
        return this.supply(() -> {
            Map<String, StoredValue> stored = this.storage.readEntries(ref, key.storageKey());
            Map<E, V> entries = HashMap.newHashMap(stored.size());
            for (Map.Entry<String, StoredValue> entry : stored.entrySet()) {
                entries.put(key.entryCodec().decode(entry.getKey()), key.valueForm().load(entry.getValue()));
            }
            // 同一个 Map 会作为快照交给所有 peekAll 的调用方
            return Collections.unmodifiableMap(entries);
        });
    }

    @NotNull
    public <T> CompletableFuture<Void> setValue(@NotNull K id, @NotNull DbKeyImpl<K, T> key, @NotNull T value) {
        EntityRef ref = this.type.ref(id);
        return this.run(() -> {
            if (setValue(this.storage, ref, key, value)) {
                this.written(id, ref, key.storageKey(), null, value);
            }
        });
    }

    // 可选键写入 Optional.empty() 就是删除. 返回存储里的数据有没有变
    static <T> boolean setValue(@NotNull EntityStorage storage, @NotNull EntityRef ref, @NotNull DbKeyImpl<?, T> key, @NotNull T value) {
        StoredValue stored = key.form().store(value);
        if (stored == null) return storage.remove(ref, key.storageKey(), null);
        storage.set(ref, key.storageKey(), null, stored);
        return true;
    }

    @NotNull
    public <T> CompletableFuture<T> updateValue(@NotNull K id, @NotNull DbKeyImpl<K, T> key, @NotNull UnaryOperator<T> operator) {
        EntityRef ref = this.type.ref(id);
        return this.supply(() -> {
            T value = updateValue(this.storage, ref, key, operator);
            this.written(id, ref, key.storageKey(), null, value);
            return value;
        });
    }

    // CAS 循环, 条件是读到的原始字节. 写入失败说明读完之后有人改过, 重读后再执行一次 operator, 所以 operator 可能执行多次
    @NotNull
    static <T> T updateValue(@NotNull EntityStorage storage, @NotNull EntityRef ref, @NotNull DbKeyImpl<?, T> key, @NotNull UnaryOperator<T> operator) {
        ValueForm<T> form = key.form();
        StorageKey storageKey = key.storageKey();
        while (true) {
            StoredValue current = storage.read(ref, storageKey, null);
            byte[] expected = current == null ? null : ((StoredValue.OfBytes) current).value();
            T value = operator.apply(form.load(current));
            if (compareAndWrite(storage, ref, storageKey, expected, form.store(value))) return value;
        }
    }

    // 可选键的 operator 返回 Optional.empty() 时 stored 为 null, 这时以读到的字节为条件删除, 本来就没有值时不用写.
    // 新字节和读到的一样时也不用写, 相当于在读到的那一刻做了一次没有改动的更新. 存储因此不会收到 expected 和新值相同的 CAS
    private static boolean compareAndWrite(@NotNull EntityStorage storage, @NotNull EntityRef ref, @NotNull StorageKey key, byte @Nullable [] expected, @Nullable StoredValue stored) {
        if (stored == null) return expected == null || storage.compareAndRemove(ref, key, null, expected);
        byte[] value = ((StoredValue.OfBytes) stored).value();
        return Arrays.equals(expected, value) || storage.compareAndSet(ref, key, null, expected, value);
    }

    @NotNull
    public CompletableFuture<Void> setCount(@NotNull K id, @NotNull StorageKey key, @Nullable String entry, long value) {
        EntityRef ref = this.type.ref(id);
        return this.run(() -> {
            this.storage.set(ref, key, entry, new StoredValue.OfLong(value));
            this.written(id, ref, key, entry, value);
        });
    }

    @NotNull
    public CompletableFuture<Long> add(@NotNull K id, @NotNull StorageKey key, @Nullable String entry, long delta) {
        EntityRef ref = this.type.ref(id);
        return this.supply(() -> {
            long value = this.storage.add(ref, key, entry, delta);
            this.written(id, ref, key, entry, value);
            return value;
        });
    }

    // 余额不足时什么都没写, 不更新快照也不广播
    @NotNull
    public CompletableFuture<OptionalLong> take(@NotNull K id, @NotNull StorageKey key, @Nullable String entry, long amount) {
        EntityRef ref = this.type.ref(id);
        return this.supply(() -> {
            OptionalLong balance = this.storage.take(ref, key, entry, amount);
            if (balance.isPresent()) {
                this.written(id, ref, key, entry, balance.getAsLong());
            }
            return balance;
        });
    }

    @NotNull
    public <V> CompletableFuture<Void> put(@NotNull K id, @NotNull DbMapKeyImpl<K, ?, V> key, @NotNull String entry, @NotNull V value) {
        EntityRef ref = this.type.ref(id);
        return this.run(() -> {
            put(this.storage, ref, key, entry, value);
            this.written(id, ref, key.storageKey(), entry, value);
        });
    }

    static <V> void put(@NotNull EntityStorage storage, @NotNull EntityRef ref, @NotNull DbMapKeyImpl<?, ?, V> key, @NotNull String entry, @NotNull V value) {
        storage.set(ref, key.storageKey(), entry, key.valueForm().store(value));
    }

    @NotNull
    public <V> CompletableFuture<Boolean> putIfAbsent(@NotNull K id, @NotNull DbMapKeyImpl<K, ?, V> key, @NotNull String entry, @NotNull V value) {
        EntityRef ref = this.type.ref(id);
        return this.supply(() -> {
            boolean inserted = putIfAbsent(this.storage, ref, key, entry, value);
            if (inserted) {
                this.written(id, ref, key.storageKey(), entry, value);
            }
            return inserted;
        });
    }

    static <V> boolean putIfAbsent(@NotNull EntityStorage storage, @NotNull EntityRef ref, @NotNull DbMapKeyImpl<?, ?, V> key, @NotNull String entry, @NotNull V value) {
        StoredValue.OfBytes stored = (StoredValue.OfBytes) key.valueForm().store(value);
        return storage.compareAndSet(ref, key.storageKey(), entry, null, stored.value());
    }

    @NotNull
    public <V> CompletableFuture<@Nullable V> updateEntry(@NotNull K id, @NotNull DbMapKeyImpl<K, ?, V> key, @NotNull String entry, @NotNull UnaryOperator<V> operator) {
        EntityRef ref = this.type.ref(id);
        return this.supply(() -> {
            V value = updateEntry(this.storage, ref, key, entry, operator);
            if (value != null) {
                this.written(id, ref, key.storageKey(), entry, value);
            }
            return value;
        });
    }

    // 和 updateValue 一样是 CAS 循环, 新字节和读到的一样时同样不写. 只改已经存在的条目, 读不到就返回 null,
    // 刚被删掉的条目不会被这次更新重新建出来
    @Nullable
    static <V> V updateEntry(@NotNull EntityStorage storage, @NotNull EntityRef ref, @NotNull DbMapKeyImpl<?, ?, V> key, @NotNull String entry, @NotNull UnaryOperator<V> operator) {
        ValueForm<V> form = key.valueForm();
        StorageKey storageKey = key.storageKey();
        while (true) {
            StoredValue current = storage.read(ref, storageKey, entry);
            if (current == null) return null;
            V value = operator.apply(form.load(current));
            byte[] expected = ((StoredValue.OfBytes) current).value();
            byte[] updated = ((StoredValue.OfBytes) form.store(value)).value();
            if (Arrays.equals(expected, updated) || storage.compareAndSet(ref, storageKey, entry, expected, updated)) return value;
        }
    }

    // 删掉的条目在快照里记成不存在时的值, 普通映射键是 null, 计数映射键是 0
    @NotNull
    public CompletableFuture<Boolean> remove(@NotNull K id, @NotNull MapKeyImpl<K, ?, ?> key, @NotNull String entry) {
        EntityRef ref = this.type.ref(id);
        return this.supply(() -> {
            boolean removed = this.storage.remove(ref, key.storageKey(), entry);
            if (removed) {
                this.written(id, ref, key.storageKey(), entry, key.valueForm().load(null));
            }
            return removed;
        });
    }

    // 写入方先用自己写进去的值更新快照, 再广播失效. 别的服靠这条失效重读; 本服自己也会收到, 再读一次就能追上并发写入的其他服
    private void written(@NotNull K id, @NotNull EntityRef ref, @NotNull StorageKey key, @Nullable String entry, @Nullable Object value) {
        this.type.snapshots().put(id, key.id(), entry, value);
        this.invalidate(ref.entityId(), key.id(), entry);
    }

    void invalidate(byte @NotNull [] entityId, @NotNull String keyId, @Nullable String entry) {
        this.messenger.invalidate(this.type.name(), entityId, keyId, entry);
    }

    @NotNull
    private <T> CompletableFuture<T> supply(@NotNull Supplier<T> action) {
        return CompletableFuture.supplyAsync(action, this.executor);
    }

    @NotNull
    private CompletableFuture<Void> run(@NotNull Runnable action) {
        return CompletableFuture.runAsync(action, this.executor);
    }
}