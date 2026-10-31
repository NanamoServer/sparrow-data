package net.momirealms.sparrow.data;

import org.jetbrains.annotations.NotNull;

/** 排行结果的一项, 实体 ID 加上用来排行的数值. */
public record RankEntry<K>(@NotNull K id, long value) {
}