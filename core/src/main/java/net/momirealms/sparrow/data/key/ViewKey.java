package net.momirealms.sparrow.data.key;

/** 数据在宿主自己的存储里的普通键. 数据层只管它的快照, 用宿主给的加载函数读取, 宿主改完数据后调用 invalidate. */
public interface ViewKey<K, T> extends ValueKey<K, T> {
}