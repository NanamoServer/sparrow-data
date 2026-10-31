package net.momirealms.sparrow.data.key.builder;

import net.momirealms.sparrow.data.key.DbMapCounterKey;
import org.jetbrains.annotations.NotNull;

/** 注册 {@link DbMapCounterKey} 的构建器, 可以设精度, 所有条目用同一个精度. */
public interface DbMapCounterKeyBuilder<K, E> {

    @NotNull
    DbMapCounterKeyBuilder<K, E> scale(int scale);

    @NotNull
    DbMapCounterKey<K, E> register();
}