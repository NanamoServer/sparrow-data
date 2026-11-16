package net.momirealms.sparrow.data.internal.key;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.key.DataKey;
import net.momirealms.sparrow.data.storage.StorageKey;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public abstract class AbstractDataKey<K> implements DataKey<K> {
    private final Key id;
    private final EntityTypeImpl<K> type;
    private final StorageKey storageKey;

    // 各种键实现的共同部分. 键注册后不可变, 同一个 ID 只有一个对象, 所以按对象身份比较
    protected AbstractDataKey(@NotNull Key id, @NotNull EntityTypeImpl<K> type, boolean sortable) {
        this.id = id;
        this.type = type;
        this.storageKey = new StorageKey(id.toString(), sortable);
    }

    @NotNull
    @Override
    public Key id() {
        return this.id;
    }

    @NotNull
    @Override
    public EntityTypeImpl<K> type() {
        return this.type;
    }

    @NotNull
    public StorageKey storageKey() {
        return this.storageKey;
    }

    // 这个键的数据是不是放在 EntityStorage 里. 视图键和名单键不是, 注册它们不需要存储做准备
    public boolean stored() {
        return true;
    }
}