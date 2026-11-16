package net.momirealms.sparrow.data.storage;

import org.jetbrains.annotations.NotNull;

/** 存储里的一个值, 有 long, double 和字节三种, 各占一列. */
public sealed interface StoredValue {

    /** memoryInt 和 memoryLong 键以及计数键的值. */
    record OfLong(long value) implements StoredValue {
    }

    /** memoryDouble 键的值. */
    record OfDouble(double value) implements StoredValue {
    }

    /** 其他键的值, 内容是键的 ValueCodec 编出来的字节. */
    record OfBytes(byte @NotNull [] value) implements StoredValue {
    }
}