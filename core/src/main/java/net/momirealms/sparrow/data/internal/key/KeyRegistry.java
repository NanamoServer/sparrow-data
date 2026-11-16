package net.momirealms.sparrow.data.internal.key;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.key.MemoryDataKey;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ApiStatus.Internal
public final class KeyRegistry<K> {
    private static final int MAX_ID_LENGTH = 64;

    private final Runnable prepareStorage;
    private volatile Table<K> table = Table.of(Map.of());

    // prepareStorage 让存储为这个实体类型做好准备, 可以重复调用. 视图键的数据不在存储里, 注册它时不调用
    public KeyRegistry(@NotNull Runnable prepareStorage) {
        this.prepareStorage = prepareStorage;
    }

    @Nullable
    public AbstractDataKey<K> get(@NotNull Key id) {
        return this.table.byId().get(id);
    }

    // 一个实体类型下的键表. 读远多于写, 所以写时复制一份新表再整体替换, 读的时候不加锁
    public synchronized void register(@NotNull AbstractDataKey<K> key) {
        String id = key.id().toString();
        if (id.length() > MAX_ID_LENGTH) throw new IllegalArgumentException("Data key id is longer than " + MAX_ID_LENGTH + " characters: " + id);
        if (id.indexOf('.') >= 0) throw new IllegalArgumentException("Data key id must not contain '.': " + id);
        if (this.table.byId().containsKey(key.id())) throw new IllegalArgumentException("Data key is already registered: " + id);
        // 键进表之前存储已经就绪, 之后任何人拿到这个键都能直接读写
        if (!(key instanceof ViewKeyImpl<?, ?>)) {
            this.prepareStorage.run();
        }
        Map<Key, AbstractDataKey<K>> byId = new HashMap<>(this.table.byId());
        byId.put(key.id(), key);
        this.table = Table.of(byId);
    }

    // 注销后同一个 ID 可以重新注册, 旧的键对象从此不再算作已注册
    public synchronized void unregister(@NotNull AbstractDataKey<K> key) {
        if (!this.isRegistered(key)) throw new IllegalArgumentException("Data key is not registered: " + key.id());
        Map<Key, AbstractDataKey<K>> byId = new HashMap<>(this.table.byId());
        byId.remove(key.id());
        this.table = Table.of(byId);
    }

    public boolean isRegistered(@NotNull AbstractDataKey<K> key) {
        return this.table.byId().get(key.id()) == key;
    }

    // online 时要加载的全部内存键, 同一张表里取出, 和注册状态保持一致
    @NotNull
    public List<AbstractDataKey<K>> memoryKeys() {
        return this.table.memoryKeys();
    }

    private record Table<K>(@NotNull Map<Key, AbstractDataKey<K>> byId, @NotNull List<AbstractDataKey<K>> memoryKeys) {

        // byId 是调用方刚复制出来的新 Map, 交出去之后不再修改. volatile 写入保证读的一方看到完整内容, 不需要再复制一次
        @NotNull
        static <K> Table<K> of(@NotNull Map<Key, AbstractDataKey<K>> byId) {
            List<AbstractDataKey<K>> memoryKeys = byId.values().stream()
                    .filter(key -> key instanceof MemoryDataKey<?>)
                    .toList();
            return new Table<>(byId, memoryKeys);
        }
    }
}