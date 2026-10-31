package net.momirealms.sparrow.data.key;

/** 可以排行的内存 long 键, 例如等级. */
public interface SortableMemoryKey<K> extends MemoryLongKey<K>, SortableKey<K> {
}