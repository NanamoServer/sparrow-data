package net.momirealms.sparrow.data.key;

/** DB 权威的普通键, 每次修改都先落库, 读改写用以旧值为条件的 CAS. */
public interface DbKey<K, T> extends ValueKey<K, T> {
}