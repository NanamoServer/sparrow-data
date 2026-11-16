package net.momirealms.sparrow.data.storage;

import org.jetbrains.annotations.NotNull;

import java.util.List;

public record SaveBatch(@NotNull List<Write> writes, @NotNull List<Remove> removes) {

    // 同一个键在一个 batch 里最多出现一次, 所以写入和删除的执行顺序无关
    public record Write(@NotNull StorageKey key, @NotNull StoredValue value) {
    }

    public record Remove(@NotNull StorageKey key) {
    }
}