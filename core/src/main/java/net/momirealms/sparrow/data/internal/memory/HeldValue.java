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

    private volatile T value;
    private boolean dirty;

    // 普通内存键在持有容器里的槽. 修改和取脏要在 HeldEntity 的锁里调用, 读值不加锁
    public HeldValue(@NotNull StorageKey key, @NotNull ValueForm<T> form, @NotNull T value) {
        this.key = key;
        this.form = form;
        this.value = value;
    }

    @NotNull
    public T value() {
        return this.value;
    }

    public void set(@NotNull T value) {
        this.value = value;
        this.dirty = true;
    }

    // 收到覆盖时调用. stored 为 null 是重置, 读成默认值或 Optional.empty()
    public void overwrite(@Nullable StoredValue stored) {
        this.set(this.form.load(stored));
    }

    // 脏的话清掉标记, 把这一刻的值编码进这次落库. 可选键的 Optional.empty() 没有要存的值, 落库时删除
    public void takeDirty(@NotNull List<SaveBatch.Write> writes, @NotNull List<SaveBatch.Remove> removes) {
        if (!this.dirty) return;
        this.dirty = false;
        StoredValue stored = this.form.store(this.value);
        if (stored == null) {
            removes.add(new SaveBatch.Remove(this.key, null));
        } else {
            writes.add(new SaveBatch.Write(this.key, null, stored));
        }
    }

    // publish 用的当前值, null 表示没有值
    @Nullable
    public StoredValue stored() {
        return this.form.store(this.value);
    }
}