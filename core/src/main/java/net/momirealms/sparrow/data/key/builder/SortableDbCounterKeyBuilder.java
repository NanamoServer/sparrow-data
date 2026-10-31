package net.momirealms.sparrow.data.key.builder;

import net.momirealms.sparrow.data.key.SortableDbCounterKey;
import org.jetbrains.annotations.NotNull;

/** 声明了 sortable 的 {@link DbCounterKeyBuilder}, 注册结果是 {@link SortableDbCounterKey}. */
public interface SortableDbCounterKeyBuilder<K> {

    @NotNull
    SortableDbCounterKeyBuilder<K> scale(int scale);

    @NotNull
    SortableDbCounterKey<K> register();
}