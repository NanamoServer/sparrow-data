package net.momirealms.sparrow.data.internal.memory;

import net.momirealms.sparrow.data.internal.key.ValueForm;
import net.momirealms.sparrow.data.key.EntryKeyCodec;
import net.momirealms.sparrow.data.storage.SaveBatch;
import net.momirealms.sparrow.data.storage.StorageKey;
import net.momirealms.sparrow.data.storage.StoredValue;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

@ApiStatus.Internal
public final class HeldEntries<E, V> {
    private final StorageKey key;
    private final EntryKeyCodec<E> entryCodec;
    private final ValueForm<V> form;

    private final Map<String, V> entries;
    private final Set<String> dirty = new HashSet<>();
    private final Set<String> removed = new HashSet<>();

    // 映射内存键在持有容器里的槽, 条目按编码后的条目键存放. 修改和取脏要在 HeldEntity 的锁里调用, 读单个条目不加锁.
    // 同一个条目只会在 dirty 和 removed 其中之一里, 记的是它最后一次操作的结果
    public HeldEntries(@NotNull StorageKey key, @NotNull EntryKeyCodec<E> entryCodec, @NotNull ValueForm<V> form, @NotNull Map<String, V> entries) {
        this.key = key;
        this.entryCodec = entryCodec;
        this.form = form;
        this.entries = new ConcurrentHashMap<>(entries);
    }

    @Nullable
    public V get(@NotNull String entry) {
        return this.entries.get(entry);
    }

    // 在锁里调用, 得到同一时刻的全部条目
    @NotNull
    public Map<E, V> getAll() {
        Map<E, V> all = HashMap.newHashMap(this.entries.size());
        this.entries.forEach((entry, value) -> all.put(this.entryCodec.decode(entry), value));
        return Collections.unmodifiableMap(all);
    }

    public void put(@NotNull String entry, @NotNull V value) {
        this.entries.put(entry, value);
        this.removed.remove(entry);
        this.dirty.add(entry);
    }

    // 条目不存在时什么都不用记, 它要么不在存储里, 要么已经记在 removed 里
    public void remove(@NotNull String entry) {
        if (this.entries.remove(entry) == null) return;
        this.dirty.remove(entry);
        this.removed.add(entry);
    }

    // 收到覆盖时调用. stored 为 null 是重置, 删掉这个条目
    public void overwrite(@NotNull String entry, @Nullable StoredValue stored) {
        if (stored == null) {
            this.remove(entry);
        } else {
            this.put(entry, this.form.load(stored));
        }
    }

    // operator 返回 null 表示删除这个条目
    @Nullable
    public V compute(@NotNull String entry, @NotNull UnaryOperator<@Nullable V> operator) {
        V value = operator.apply(this.entries.get(entry));
        if (value == null) {
            this.remove(entry);
        } else {
            this.put(entry, value);
        }
        return value;
    }

    // 清掉脏标记, 取出脏条目这一刻的值和删掉的条目, 编码留到出锁以后
    public void takeDirty(@NotNull List<HeldEntity.Dirty> taken) {
        if (this.dirty.isEmpty() && this.removed.isEmpty()) return;
        Map<String, V> written = HashMap.newHashMap(this.dirty.size());
        for (String entry : this.dirty) {
            written.put(entry, this.entries.get(entry));
        }
        List<String> deleted = List.copyOf(this.removed);
        this.dirty.clear();
        this.removed.clear();
        taken.add((writes, removes) -> {
            written.forEach((entry, value) -> writes.add(new SaveBatch.Write(this.key, entry, this.form.store(value))));
            int deletedCount = deleted.size();
            for (int i = 0; i < deletedCount; i++) {
                removes.add(new SaveBatch.Remove(this.key, deleted.get(i)));
            }
        });
    }

    // publish 用, 在锁里调用, 编码全部条目
    @NotNull
    public Map<String, StoredValue> storedEntries() {
        Map<String, StoredValue> stored = HashMap.newHashMap(this.entries.size());
        this.entries.forEach((entry, value) -> stored.put(entry, this.form.store(value)));
        return stored;
    }
}