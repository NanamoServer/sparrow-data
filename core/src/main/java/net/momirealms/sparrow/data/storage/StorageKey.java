package net.momirealms.sparrow.data.storage;

import org.jetbrains.annotations.NotNull;

/** 在存储里定位一个键. sortable 为 true 时, 实现方要同时维护这个键的排行数据. */
public record StorageKey(@NotNull String id, boolean sortable) {
}