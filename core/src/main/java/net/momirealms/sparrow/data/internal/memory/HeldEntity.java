package net.momirealms.sparrow.data.internal.memory;

import net.momirealms.sparrow.data.internal.key.AbstractDataKey;
import net.momirealms.sparrow.data.internal.key.MapKeyImpl;
import net.momirealms.sparrow.data.internal.key.ValueForm;
import net.momirealms.sparrow.data.internal.key.ValueKeyImpl;
import net.momirealms.sparrow.data.storage.EntityRef;
import net.momirealms.sparrow.data.storage.EntityStorage;
import net.momirealms.sparrow.data.storage.SaveBatch;
import net.momirealms.sparrow.data.storage.StoredEntity;
import net.momirealms.sparrow.data.storage.StoredValue;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.UnaryOperator;

@ApiStatus.Internal
public final class HeldEntity<K> {
    private final EntityRef ref;

    private final Map<AbstractDataKey<K>, HeldValue<?>> values;
    private final Map<AbstractDataKey<K>, HeldEntries<?, ?>> entries;

    private final TaskChain saves = new TaskChain();
    private final TaskChain publishes = new TaskChain();
    private boolean released;

    // 一个实体在本服持有期间的全部内存键值, 用这个对象的锁把所有修改, 落库取脏和 publish 取值串起来. 读单个值和单个条目不加锁, 值都是不可变对象.
    // 没有存储值的键, 有默认值的用默认值, 可选键用 Optional.empty()
    public HeldEntity(@NotNull EntityRef ref, @NotNull List<AbstractDataKey<K>> keys, @NotNull StoredEntity stored) {
        this.ref = ref;
        int size = keys.size();
        this.values = HashMap.newHashMap(size);
        this.entries = HashMap.newHashMap(size);
        for (int i = 0; i < size; i++) {
            AbstractDataKey<K> key = keys.get(i);
            String keyId = key.storageKey().id();
            if (key instanceof MapKeyImpl<?, ?, ?> map) {
                this.entries.put(key, loadEntries(map, stored.entries().getOrDefault(keyId, Map.of())));
            } else {
                this.values.put(key, loadValue((ValueKeyImpl<?, ?>) key, stored.values().get(keyId)));
            }
        }
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

    // 锁里只取出脏的部分并清掉标记, 编码在锁外进行, 写入不用等编码. 没有脏数据时返回 null
    @Nullable
    public SaveBatch takeDirty() {
        List<HeldValue.Dirty<?>> dirtyValues = new ArrayList<>();
        List<HeldEntries.Dirty<?>> dirtyEntries = new ArrayList<>();
        synchronized (this) {
            for (HeldValue<?> slot : this.values.values()) {
                HeldValue.Dirty<?> dirty = slot.takeDirty();
                if (dirty != null) dirtyValues.add(dirty);
            }
            for (HeldEntries<?, ?> slot : this.entries.values()) {
                HeldEntries.Dirty<?> dirty = slot.takeDirty();
                if (dirty != null) dirtyEntries.add(dirty);
            }
        }
        if (dirtyValues.isEmpty() && dirtyEntries.isEmpty()) return null;
        List<SaveBatch.Write> writes = new ArrayList<>();
        List<SaveBatch.Remove> removes = new ArrayList<>();
        int valueCount = dirtyValues.size();
        for (int i = 0; i < valueCount; i++) {
            dirtyValues.get(i).encode(writes, removes);
        }
        int entryCount = dirtyEntries.size();
        for (int i = 0; i < entryCount; i++) {
            dirtyEntries.get(i).encode(writes, removes);
        }
        return new SaveBatch(writes, removes);
    }

    // 接在上一次落库后面, 轮到时在 executor 上先取脏再交给存储. 取脏发生在上一次存储调用结束之后, 所以写进库的顺序就是取脏的顺序
    @NotNull
    public CompletableFuture<Void> save(@NotNull EntityStorage storage, @NotNull Executor executor) {
        return this.saves.then(() -> CompletableFuture.runAsync(() -> this.saveDirty(storage), executor));
    }

    // 排进 publish 链, 轮到时才取值, 上一次的副本写完才轮到下一次, 所以同一个键按调用顺序写进 Redis.
    // 检查和排队在同一把锁里, 释放之后不会再有 publish 排到 offline 的最后一次 publish 后面
    @NotNull
    public synchronized CompletableFuture<Void> publish(@NotNull ReplicaPublisher publisher, @NotNull AbstractDataKey<K> key) {
        this.requireHeld();
        return this.publishes.then(() -> publisher.publish(this, key));
    }

    // saveAndPublish 用, 只 publish 本次持有期间 publish 过的键
    @NotNull
    public synchronized CompletableFuture<Void> publishAll(@NotNull ReplicaPublisher publisher) {
        this.requireHeld();
        return this.publishes.then(() -> publisher.publishAll(this));
    }

    // 在锁里取出要 publish 的这个键. 没有要写的时候两个列表都是空的
    @NotNull
    public synchronized Publications takePublication(@NotNull AbstractDataKey<K> key, long now, long refreshNanos) {
        Publications publications = Publications.empty();
        HeldValue<?> value = this.values.get(key);
        if (value != null) {
            publications.add(value.takePublication(now, refreshNanos));
        } else {
            publications.add(this.entries.get(key).takePublication(now, refreshNanos));
        }
        return publications;
    }

    // 在锁里取出本次持有期间 publish 过的键
    @NotNull
    public synchronized Publications takePublications(long now, long refreshNanos) {
        Publications publications = Publications.empty();
        for (HeldValue<?> slot : this.values.values()) {
            if (slot.published()) publications.add(slot.takePublication(now, refreshNanos));
        }
        for (HeldEntries<?, ?> slot : this.entries.values()) {
            if (slot.published()) publications.add(slot.takePublication(now, refreshNanos));
        }
        return publications;
    }

    // offline 时调用. 之后的修改和 publish 都抛 IllegalStateException, 包括释放之前就拿到了这个容器的调用方.
    // 同时把最后一次 publish 排进链里, 返回的 future 在它写完副本后结束; 释放之前的修改由随后的落库带走
    @NotNull
    public synchronized CompletableFuture<Void> release(@NotNull ReplicaPublisher publisher) {
        this.released = true;
        return this.publishes.then(() -> publisher.publishAll(this));
    }

    @NotNull
    public EntityRef ref() {
        return this.ref;
    }

    private void saveDirty(@NotNull EntityStorage storage) {
        SaveBatch batch = this.takeDirty();
        if (batch == null) return;
        storage.save(this.ref, batch);
    }

    private void requireHeld() {
        if (this.released) throw new IllegalStateException("Entity is no longer held by this server: " + this.ref.entityType());
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

    // 一次 publish 要写的普通键和映射键, 在锁里取出, 编码和发送在锁外
    public record Publications(@NotNull List<HeldValue.Publication<?>> values, @NotNull List<HeldEntries.Publication<?>> entries) {

        @NotNull
        static Publications empty() {
            return new Publications(new ArrayList<>(), new ArrayList<>());
        }

        void add(@Nullable HeldValue.Publication<?> value) {
            if (value != null) {
                this.values.add(value);
            }
        }

        void add(@Nullable HeldEntries.Publication<?> entries) {
            if (entries != null) {
                this.entries.add(entries);
            }
        }
    }
}