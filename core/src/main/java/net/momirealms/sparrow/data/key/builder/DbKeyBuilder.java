package net.momirealms.sparrow.data.key.builder;

import net.momirealms.sparrow.data.key.DbKey;
import org.jetbrains.annotations.NotNull;

/** 注册 {@link DbKey} 的构建器. DB 键不需要补读, register 立即返回. */
public interface DbKeyBuilder<K, T> {

    @NotNull
    DbKey<K, T> register();
}