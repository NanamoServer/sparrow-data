package net.momirealms.sparrow.data.internal.memory;

import net.momirealms.sparrow.data.internal.key.AbstractDataKey;
import net.momirealms.sparrow.data.internal.key.MapKeyImpl;
import net.momirealms.sparrow.data.internal.key.ValueForm;
import net.momirealms.sparrow.data.internal.key.ValueKeyImpl;
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
import java.util.function.UnaryOperator;

@ApiStatus.Internal
public final class HeldEntity<K> {
    private final Map<AbstractDataKey<K>, HeldValue<?>> values;
    private final Map<AbstractDataKey<K>, HeldEntries<?, ?>> entries;

    // 一个实体在本服持有期间的全部内存键值, 用这个对象的锁把所有修改和落库取脏串起来. 读单个值和单个条目不加锁, 值都是不可变对象.
    // 没有存储值的键, 有默认值的用默认值, 可选键用 Optional.empty()
    public HeldEntity(@NotNull List<AbstractDataKey<K>> keys, @NotNull StoredEntity stored) {
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
        this.value(key).set(value);
    }

    // operator 在锁里执行, 同一个实体上的其他修改和落库取脏都要等它
    @NotNull
    public synchronized <T> T update(@NotNull ValueKeyImpl<K, T> key, @NotNull UnaryOperator<T> operator) {
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
        this.entries(key).put(entry, value);
    }

    public synchronized void remove(@NotNull MapKeyImpl<K, ?, ?> key, @NotNull String entry) {
        this.entries(key).remove(entry);
    }

    @Nullable
    public synchronized <V> V compute(@NotNull MapKeyImpl<K, ?, V> key, @NotNull String entry, @NotNull UnaryOperator<@Nullable V> operator) {
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
}