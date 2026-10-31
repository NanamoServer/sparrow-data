package net.momirealms.sparrow.data.key.builder;

import net.momirealms.sparrow.data.key.MemoryLongKey;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

/** 注册 {@link MemoryLongKey} 的构建器. 这种键直接以 long 存储, 默认值不设时为 0, 可以设精度, 也可以声明为 sortable. */
public interface MemoryLongKeyBuilder<K> {

    @NotNull
    MemoryLongKeyBuilder<K> defaultValue(long value);

    @NotNull
    MemoryLongKeyBuilder<K> scale(int scale);

    @NotNull
    SortableMemoryLongKeyBuilder<K> sortable();

    @NotNull
    CompletableFuture<MemoryLongKey<K>> register();
}