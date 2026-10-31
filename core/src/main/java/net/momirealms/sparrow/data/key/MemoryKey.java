package net.momirealms.sparrow.data.key;

/** 内存权威的普通键, 最新值在持有者的内存里, 存储只是 checkpoint. */
public interface MemoryKey<K, T> extends ValueKey<K, T>, MemoryDataKey<K> {
}