package net.momirealms.sparrow.data.key.builder;

import net.momirealms.sparrow.data.key.SortableMemoryLongKey;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

/** 声明了 sortable 的 {@link MemoryLongKeyBuilder}, 注册结果是 {@link SortableMemoryLongKey}. */
public interface SortableMemoryLongKeyBuilder<K> {

    @NotNull
    SortableMemoryLongKeyBuilder<K> defaultValue(long value);

    @NotNull
    SortableMemoryLongKeyBuilder<K> scale(int scale);

    @NotNull
    CompletableFuture<SortableMemoryLongKey<K>> register();
}