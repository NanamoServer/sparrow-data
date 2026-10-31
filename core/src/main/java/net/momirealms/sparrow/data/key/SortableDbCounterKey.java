package net.momirealms.sparrow.data.key;

/** 可以排行的 DB 计数键, 例如货币. */
public interface SortableDbCounterKey<K> extends DbCounterKey<K>, SortableKey<K> {
}