package net.momirealms.sparrow.data.key;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.entity.EntityType;
import org.jetbrains.annotations.NotNull;

/** 所有数据键的根类型, 带有键 ID 和它所属的实体类型. */
public interface DataKey<K> {

    @NotNull
    Key id();

    @NotNull
    EntityType<K> type();
}