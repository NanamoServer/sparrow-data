package net.momirealms.sparrow.data.key.builder;

import net.momirealms.sparrow.data.key.SortableMemoryDoubleKey;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

/** 声明了 sortable 的 {@link MemoryDoubleKeyBuilder}, 注册结果是 {@link SortableMemoryDoubleKey}. */
public interface SortableMemoryDoubleKeyBuilder<K> {

    @NotNull
    SortableMemoryDoubleKeyBuilder<K> defaultValue(double value);

    @NotNull
    CompletableFuture<SortableMemoryDoubleKey<K>> register();
}