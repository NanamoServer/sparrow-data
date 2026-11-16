package net.momirealms.sparrow.data.key.builder;

import net.momirealms.sparrow.data.key.MemoryIntKey;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

/** 注册 {@link MemoryIntKey} 的构建器. 默认值不设时为 0, 可以声明为 sortable. */
public interface MemoryIntKeyBuilder<K> {

    @NotNull
    MemoryIntKeyBuilder<K> defaultValue(int value);

    @NotNull
    SortableMemoryIntKeyBuilder<K> sortable();

    @NotNull
    CompletableFuture<MemoryIntKey<K>> register();
}