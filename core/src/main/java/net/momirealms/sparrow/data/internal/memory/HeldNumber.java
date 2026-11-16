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
public final class HeldNumber<T> implements HeldSlot<T> {
    private final StorageKey key;
    private final ValueForm.NumberForm<T> form;

    private volatile long bits; // int 和 long 键是数值本身, double 键是它的位模式
    private boolean cleared;    // 最近一次写入的是不是 null, 落库时据此删除这一行
    private boolean dirty;

    // int, long, double 键的槽. 值放在一个 long 里, 走 bits 和 setBits 的读写不产生对象
    public HeldNumber(@NotNull StorageKey key, @NotNull ValueForm.NumberForm<T> form, long bits) {
        this.key = key;
        this.form = form;
        this.bits = bits;
    }

    public long bits() {
        return this.bits;
    }

    public void setBits(long bits) {
        this.bits = bits;
        this.cleared = false;
        this.dirty = true;
    }

    @NotNull
    @Override
    public T value() {
        return this.form.fromBits(this.bits);
    }

    @Override
    public void set(@Nullable T value) {
        if (value != null) {
            this.setBits(this.form.toBits(value));
            return;
        }
        this.bits = this.form.fallbackBits();
        this.cleared = true;
        this.dirty = true;
    }

    @Override
    public void overwrite(@Nullable StoredValue stored) {
        if (stored == null) {
            this.set(null);
        } else {
            this.setBits(this.form.loadBits(stored));
        }
    }

    @Override
    public void takeDirty(@NotNull List<HeldEntity.Dirty> taken) {
        if (!this.dirty) return;
        this.dirty = false;
        long bits = this.bits;
        boolean cleared = this.cleared;
        taken.add((writes, removes) -> {
            if (cleared) {
                removes.add(new SaveBatch.Remove(this.key));
            } else {
                writes.add(new SaveBatch.Write(this.key, this.form.storeBits(bits)));
            }
        });
    }

    @NotNull
    @Override
    public StoredValue stored() {
        return this.form.storeBits(this.bits);
    }
}