package net.momirealms.sparrow.data.storage;

import org.jetbrains.annotations.NotNull;

/** 浮点键的排行查询返回的一行, 编码后的实体 ID 加数值. */
public record DoubleRankRow(byte @NotNull [] entityId, double value) {
}