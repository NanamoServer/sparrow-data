package net.momirealms.sparrow.data.key;

/** 值是 int 的内存键. 这种键直接存 int, 读写不装箱, 用 Entity 上接收这种键的 get, write, add 和 updateInt. */
public interface MemoryIntKey<K> extends MemoryKey<K, Integer> {
}