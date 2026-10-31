package net.momirealms.sparrow.data.key.builder;

import net.momirealms.sparrow.data.key.MemoryKey;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

/** 注册 {@link MemoryKey} 的构建器. 运行中注册要对已持有的实体补读, 所以 register 返回 future. */
public interface MemoryKeyBuilder<K, T> {

    @NotNull
    CompletableFuture<MemoryKey<K, T>> register();
}