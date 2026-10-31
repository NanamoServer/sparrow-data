package net.momirealms.sparrow.data.storage;

import org.jetbrains.annotations.NotNull;

/** 在存储里定位一个实体, 由实体类型名和编码后的实体 ID 组成. */
public record EntityRef(@NotNull String entityType, byte @NotNull [] entityId) {
}