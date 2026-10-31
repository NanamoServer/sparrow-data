package net.momirealms.sparrow.data.key.builder;

import net.momirealms.sparrow.data.key.SortableMemoryKey;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

/** 声明了 sortable 的 {@link MemoryLongKeyBuilder}, 注册结果是 {@link SortableMemoryKey}. */
public interface SortableMemoryLongKeyBuilder<K> {

    @NotNull
    SortableMemoryLongKeyBuilder<K> defaultValue(long value);

    @NotNull
    CompletableFuture<SortableMemoryKey<K>> register();
}