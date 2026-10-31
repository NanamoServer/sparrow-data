package net.momirealms.sparrow.data.storage;

import org.jetbrains.annotations.NotNull;

/** 存储里的一个值, 只有 long 和 NBT 二进制两种. */
public sealed interface StoredValue {

    /** memoryLong 键和两种计数键的值. */
    record OfLong(long value) implements StoredValue {
    }

    /** 其他键的值, 内容是 NBT 二进制. */
    record OfBytes(byte @NotNull [] value) implements StoredValue {
    }
}