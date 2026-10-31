package net.momirealms.sparrow.data.key;

/** DB 权威的按条目计数键, 每个条目是一个 long, 不存在的条目视为 0. */
public interface DbMapCounterKey<K, E> extends MapKey<K, E, Long> {
}