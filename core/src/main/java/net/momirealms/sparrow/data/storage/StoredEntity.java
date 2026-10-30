package net.momirealms.sparrow.data.storage;

import org.jetbrains.annotations.NotNull;

import java.util.Map;

public record StoredEntity(
        @NotNull Map<String, StoredValue> values,                 // 键 ID -> 普通键的值, 没有存储值的键不在里面
        @NotNull Map<String, Map<String, StoredValue>> entries    // 键 ID -> 条目键 -> 值, 没有条目的键不在里面
) {
}