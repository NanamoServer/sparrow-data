package net.momirealms.sparrow.data.key;

/** 值是 double 的内存键. 这种键直接存 double, 读写不装箱, 用 Entity 上接收这种键的 get, write, add 和 updateDouble. */
public interface MemoryDoubleKey<K> extends MemoryKey<K, Double> {
}