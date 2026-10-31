package net.momirealms.sparrow.data.key;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public final class ValueCodecs {
    public static final ValueCodec<Boolean> BOOLEAN = ValueCodec.of(
            value -> {
                return new byte[]{(byte) (value ? 1 : 0)};
            }, bytes -> {
                requireLength(bytes, 1);
                return bytes[0] != 0;
            });

    public static final ValueCodec<Integer> INT = ValueCodec.of(
            value -> {
                return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
            }, bytes -> {
                requireLength(bytes, Integer.BYTES);
                return ByteBuffer.wrap(bytes).getInt();
            });

    public static final ValueCodec<Long> LONG = ValueCodec.of(
            value -> {
                return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
            }, bytes -> {
                requireLength(bytes, Long.BYTES);
                return ByteBuffer.wrap(bytes).getLong();
            });

    // doubleToLongBits 把所有 NaN 换成同一个位模式, 同一个值编出的字节才稳定
    public static final ValueCodec<Double> DOUBLE = ValueCodec.of(
            value -> {
                return ByteBuffer.allocate(Long.BYTES).putLong(Double.doubleToLongBits(value)).array();
            }, bytes -> {
                requireLength(bytes, Long.BYTES);
                return Double.longBitsToDouble(ByteBuffer.wrap(bytes).getLong());
            });

    public static final ValueCodec<String> STRING = ValueCodec.of(
            value -> {
                return value.getBytes(StandardCharsets.UTF_8);
            }, bytes -> {
                return new String(bytes, StandardCharsets.UTF_8);
            });

    public static final ValueCodec<UUID> UUID = ValueCodec.of(
            value -> {
                return ByteBuffer.allocate(16)
                        .putLong(value.getMostSignificantBits())
                        .putLong(value.getLeastSignificantBits())
                        .array();
            }, bytes -> {
                requireLength(bytes, 16);
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                // 构造参数从左到右求值, 先读出的是高 64 位
                return new UUID(buffer.getLong(), buffer.getLong());
            });

    // 编码和解码都复制一份, 调用方之后改自己手里的数组不会影响数据层
    public static final ValueCodec<byte[]> BYTES = ValueCodec.of(
            value -> {
                return value.clone();
            }, bytes -> {
                return bytes.clone();
            });

    private ValueCodecs() {
    }

    private static void requireLength(byte @NotNull [] bytes, int length) {
        if (bytes.length != length) throw new IllegalArgumentException("Expected " + length + " bytes but got " + bytes.length);
    }
}