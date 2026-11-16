package net.momirealms.sparrow.data.key.builder;

import net.momirealms.sparrow.data.key.ViewKey;
import org.jetbrains.annotations.NotNull;

/** 注册 {@link ViewKey} 的构建器. 视图键不需要补读, register 立即返回. */
public interface ViewKeyBuilder<K, T> {

    @NotNull
    ViewKey<K, T> register();
}