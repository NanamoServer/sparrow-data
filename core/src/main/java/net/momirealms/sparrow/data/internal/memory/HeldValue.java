package net.momirealms.sparrow.data.internal.memory;

import net.momirealms.sparrow.data.internal.key.ValueForm;
import net.momirealms.sparrow.data.storage.SaveBatch;
import net.momirealms.sparrow.data.storage.StorageKey;
import net.momirealms.sparrow.data.storage.StoredValue;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

@ApiStatus.Internal
public final class HeldValue<T> {
    private final StorageKey key;
    private final ValueForm<T> form;

    private volatile @Nullable T value; // 读取方看到的值. 没有存储值时是默认值, 没有默认值的键是 null
    private boolean cleared;            // 最近一次写入的是不是 null, 落库时据此删除这一行
    private boolean dirty;

    // 内存键在持有容器里的槽. 修改和取脏要在 HeldEntity 的锁里调用, 读值不加锁
    public HeldValue(@NotNull StorageKey key, @NotNull ValueForm<T> form, @Nullable T value) {
        this.key = key;
        this.form = form;
        this.value = value;
    }

    @Nullable
    public T value() {
        return this.value;
    }

    // value 为 null 表示删除存储的值, 之后读到默认值
    public void set(@Nullable T value) {
        this.value = this.form.orFallback(value);
        this.cleared = value == null;
        this.dirty = true;
    }

    // 收到覆盖时调用. stored 为 null 是删除存储的值
    public void overwrite(@Nullable StoredValue stored) {
        this.set(stored == null ? null : this.form.load(stored));
    }

    // 脏的话清掉标记, 取出这一刻的值, 编码留到出锁以后
    public void takeDirty(@NotNull List<HeldEntity.Dirty> taken) {
        if (!this.dirty) return;
        this.dirty = false;
        T value = this.cleared ? null : this.value;
        taken.add((writes, removes) -> {
            StoredValue stored = this.form.store(value);
            if (stored == null) {
                removes.add(new SaveBatch.Remove(this.key));
            } else {
                writes.add(new SaveBatch.Write(this.key, stored));
            }
        });
    }

    // publish 用的当前值, null 表示没有值
    @Nullable
    public StoredValue stored() {
        return this.form.store(this.value);
    }
}