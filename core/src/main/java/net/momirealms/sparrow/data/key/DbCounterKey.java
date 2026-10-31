package net.momirealms.sparrow.data.key;

/** DB 权威的 long 计数键, 支持原子加减, 不存在时视为 0. */
public interface DbCounterKey<K> extends ValueKey<K, Long>, ScaledKey {
}