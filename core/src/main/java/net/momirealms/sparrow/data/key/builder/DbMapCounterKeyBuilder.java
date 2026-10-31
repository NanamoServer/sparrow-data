package net.momirealms.sparrow.data.key.builder;

import net.momirealms.sparrow.data.key.DbMapCounterKey;
import org.jetbrains.annotations.NotNull;

/** 注册 {@link DbMapCounterKey} 的构建器. */
public interface DbMapCounterKeyBuilder<K, E> {

    @NotNull
    DbMapCounterKey<K, E> register();
}