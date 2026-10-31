package net.momirealms.sparrow.data.key.builder;

import net.momirealms.sparrow.data.key.MemoryMapKey;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

/** 注册 {@link MemoryMapKey} 的构建器. 和其他内存键一样, register 返回的 future 完成后才能使用. */
public interface MemoryMapKeyBuilder<K, E, V> {

    @NotNull
    CompletableFuture<MemoryMapKey<K, E, V>> register();
}