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
public final class HeldValue<T> implements HeldSlot<T> {
    private final StorageKey key;
    private final ValueForm<T> form;

    private volatile @Nullable T value; // 读取方看到的值. 没有存储值时是默认值, 没有默认值的键是 null
    private boolean cleared;            // 最近一次写入的是不是 null, 落库时据此删除这一行
    private boolean dirty;

    // 值是对象的内存键的槽
    public HeldValue(@NotNull StorageKey key, @NotNull ValueForm<T> form, @Nullable T value) {
        this.key = key;
        this.form = form;
        this.value = value;
    }

    @Nullable
    @Override
    public T value() {
        return this.value;
    }

    @Override
    public void set(@Nullable T value) {
        this.value = this.form.orFallback(value);
        this.cleared = value == null;
        this.dirty = true;
    }

    @Override
    public void overwrite(@Nullable StoredValue stored) {
        this.set(stored == null ? null : this.form.load(stored));
    }

    @Override
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

    @Nullable
    @Override
    public StoredValue stored() {
        return this.form.store(this.value);
    }
}