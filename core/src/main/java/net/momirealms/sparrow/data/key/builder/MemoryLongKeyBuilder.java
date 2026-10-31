package net.momirealms.sparrow.data.key.builder;

import net.momirealms.sparrow.data.key.MemoryKey;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

/** 注册值为 long 的 {@link MemoryKey} 的构建器. 这种键直接以 long 存储, 默认值不设时为 0, 可以声明为 sortable. */
public interface MemoryLongKeyBuilder<K> {

    @NotNull
    MemoryLongKeyBuilder<K> defaultValue(long value);

    @NotNull
    SortableMemoryLongKeyBuilder<K> sortable();

    @NotNull
    CompletableFuture<MemoryKey<K, Long>> register();
}