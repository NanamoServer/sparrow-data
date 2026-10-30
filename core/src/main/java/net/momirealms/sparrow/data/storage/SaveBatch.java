package net.momirealms.sparrow.data.storage;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public record SaveBatch(@NotNull List<Write> writes, @NotNull List<Remove> removes) {

    // 同一个键和条目在一个 batch 里最多出现一次, 所以写入和删除的执行顺序无关. entry 为 null 表示普通键
    public record Write(@NotNull StorageKey key, @Nullable String entry, @NotNull StoredValue value) {
    }

    public record Remove(@NotNull StorageKey key, @Nullable String entry) {
    }
}