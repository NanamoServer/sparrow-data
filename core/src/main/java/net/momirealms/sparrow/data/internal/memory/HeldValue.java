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

    private boolean published;            // 本次持有期间 publish 过
    private boolean changedSincePublish;
    private long publishedAt;             // 上次 publish 取值时的 System.nanoTime()

    // 普通内存键在持有容器里的槽. 修改, 取脏和取 publish 的值要在 HeldEntity 的锁里调用, 读值不加锁
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
        this.changedSincePublish = true;
    }

    // 清掉脏标记, 带走这一刻的值. 之后的写入会重新标脏, 下一次落库再写
    @Nullable
    public Dirty<T> takeDirty() {
        if (!this.dirty) return null;
        this.dirty = false;
        return new Dirty<>(this.key, this.form, this.value);
    }

    public boolean published() {
        return this.published;
    }

    // 第一次 publish, 上次 publish 之后改过, 或者距上次 publish 超过 refreshNanos(副本可能快过期了)时, 带走这一刻的值;
    // 其他情况副本里已经是当前值, 返回 null
    @Nullable
    public Publication<T> takePublication(long now, long refreshNanos) {
        if (this.published && !this.changedSincePublish && now - this.publishedAt <= refreshNanos) return null;
        this.published = true;
        this.changedSincePublish = false;
        this.publishedAt = now;
        return new Publication<>(this.key, this.form, this.value);
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

    public record Publication<T>(@NotNull StorageKey key, @NotNull ValueForm<T> form, @NotNull T value) {
    }
}