package net.momirealms.sparrow.data.key;

/** 声明了 sortable 的 {@link MemoryIntKey}, 可以排行. */
public interface SortableMemoryIntKey<K> extends MemoryIntKey<K>, SortableKey<K> {
}