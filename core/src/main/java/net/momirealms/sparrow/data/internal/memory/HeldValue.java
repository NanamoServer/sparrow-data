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

    // 清掉脏标记, 带走这一刻的值. 之后的写入会重新标脏, 下一次落库再写
    @Nullable
    public Dirty<T> takeDirty() {
        if (!this.dirty) return null;
        this.dirty = false;
        return new Dirty<>(this.key, this.form, this.value);
    }

    public record Dirty<T>(@NotNull StorageKey key, @NotNull ValueForm<T> form, @NotNull T value) {

        // 在锁外编码. 可选键的 Optional.empty() 没有要存的值, 落库时删除
        public void encode(@NotNull List<SaveBatch.Write> writes, @NotNull List<SaveBatch.Remove> removes) {
            StoredValue stored = this.form.store(this.value);
            if (stored == null) {
                removes.add(new SaveBatch.Remove(this.key, null));
            } else {
                writes.add(new SaveBatch.Write(this.key, null, stored));
            }
        }
    }
}