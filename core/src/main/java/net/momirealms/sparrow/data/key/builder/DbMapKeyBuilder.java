package net.momirealms.sparrow.data.key.builder;

import net.momirealms.sparrow.data.key.DbMapKey;
import org.jetbrains.annotations.NotNull;

/** 注册 {@link DbMapKey} 的构建器. */
public interface DbMapKeyBuilder<K, E, V> {

    @NotNull
    DbMapKey<K, E, V> register();
}