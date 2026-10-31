package net.momirealms.sparrow.data.key.builder;

import net.momirealms.sparrow.data.key.DbCounterKey;
import org.jetbrains.annotations.NotNull;

/** 注册 {@link DbCounterKey} 的构建器, 可以声明为 sortable. */
public interface DbCounterKeyBuilder<K> {

    @NotNull
    SortableDbCounterKeyBuilder<K> sortable();

    @NotNull
    DbCounterKey<K> register();
}