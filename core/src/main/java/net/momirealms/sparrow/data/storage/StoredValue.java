package net.momirealms.sparrow.data.storage;

import org.jetbrains.annotations.NotNull;

/** 存储里的一个值, 只有 long 和字节两种. */
public sealed interface StoredValue {

    /** memoryLong 键和两种计数键的值. */
    record OfLong(long value) implements StoredValue {
    }

    /** 其他键的值, 内容是键的 ValueCodec 编出来的字节. */
    record OfBytes(byte @NotNull [] value) implements StoredValue {
    }
}