package net.momirealms.sparrow.data.storage;

import org.jetbrains.annotations.NotNull;

import java.util.Map;

// values 是键 ID -> 值, 没有存储值的键不在里面
public record StoredEntity(@NotNull Map<String, StoredValue> values) {
}