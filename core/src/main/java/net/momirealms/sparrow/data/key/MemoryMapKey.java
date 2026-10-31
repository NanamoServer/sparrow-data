package net.momirealms.sparrow.data.key;

/** 内存权威的映射键, 按条目修改, 记脏和落库. */
public interface MemoryMapKey<K, E, V> extends MapKey<K, E, V>, MemoryDataKey<K> {
}