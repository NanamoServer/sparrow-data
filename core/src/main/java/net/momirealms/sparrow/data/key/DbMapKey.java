package net.momirealms.sparrow.data.key;

/** DB 权威的映射键, 每个条目单独做原子修改. */
public interface DbMapKey<K, E, V> extends MapKey<K, E, V> {
}