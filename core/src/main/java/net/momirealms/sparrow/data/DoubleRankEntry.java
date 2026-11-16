package net.momirealms.sparrow.data;

import org.jetbrains.annotations.NotNull;

public record DoubleRankEntry<K>(@NotNull K id, double value) {
}