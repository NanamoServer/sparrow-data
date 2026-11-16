package net.momirealms.sparrow.data.key.builder;

import net.momirealms.sparrow.data.key.SortableMemoryIntKey;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

/** 声明了 sortable 的 {@link MemoryIntKeyBuilder}, 注册结果是 {@link SortableMemoryIntKey}. */
public interface SortableMemoryIntKeyBuilder<K> {

    @NotNull
    SortableMemoryIntKeyBuilder<K> defaultValue(int value);

    @NotNull
    CompletableFuture<SortableMemoryIntKey<K>> register();
}