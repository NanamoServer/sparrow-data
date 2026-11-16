package net.momirealms.sparrow.data.key;

/** 全网共享的名单键. 记录存在 Redis 里, 归写入它的服务器所有, 每台服务器在内存里有一份完整镜像, 查询是同步的. */
public interface RosterKey<K, V> extends DataKey<K> {
}