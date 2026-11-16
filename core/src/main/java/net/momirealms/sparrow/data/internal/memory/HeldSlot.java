package net.momirealms.sparrow.data.internal.memory;

import net.momirealms.sparrow.data.storage.StoredValue;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/** 内存键在持有容器里的槽. 修改和取脏要在 HeldEntity 的锁里调用, 读值不加锁. */
@ApiStatus.Internal
public sealed interface HeldSlot<T> permits HeldValue, HeldNumber {

    // 读取方看到的值. 没有存储值时是默认值, 没有默认值的键是 null
    @Nullable
    T value();

    // value 为 null 表示删除存储的值, 之后读到默认值
    void set(@Nullable T value);

    // 收到覆盖时调用. stored 为 null 是删除存储的值
    void overwrite(@Nullable StoredValue stored);

    // 脏的话清掉标记, 取出这一刻的值, 编码留到出锁以后
    void takeDirty(@NotNull List<HeldEntity.Dirty> taken);

    // publish 用的当前值, null 表示没有值
    @Nullable
    StoredValue stored();
}