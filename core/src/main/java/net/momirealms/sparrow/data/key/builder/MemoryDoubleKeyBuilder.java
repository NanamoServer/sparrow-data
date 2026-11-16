package net.momirealms.sparrow.data.key.builder;

import net.momirealms.sparrow.data.key.MemoryDoubleKey;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

/** 注册 {@link MemoryDoubleKey} 的构建器. 默认值不设时为 0, 可以声明为 sortable. */
public interface MemoryDoubleKeyBuilder<K> {

    @NotNull
    MemoryDoubleKeyBuilder<K> defaultValue(double value);

    @NotNull
    SortableMemoryDoubleKeyBuilder<K> sortable();

    @NotNull
    CompletableFuture<MemoryDoubleKey<K>> register();
}