package net.momirealms.sparrow.data.entity;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.function.Function;

public interface EntityIdCodec<K> {
    EntityIdCodec<UUID> UUID = of(EntityIdCodec::encodeUuid, EntityIdCodec::decodeUuid);
    EntityIdCodec<String> STRING = of(id -> id.getBytes(StandardCharsets.UTF_8), bytes -> new String(bytes, StandardCharsets.UTF_8));
    EntityIdCodec<Integer> INT = of(id -> ByteBuffer.allocate(Integer.BYTES).putInt(id).array(), bytes -> ByteBuffer.wrap(bytes).getInt());
    EntityIdCodec<Long> LONG = of(id -> ByteBuffer.allocate(Long.BYTES).putLong(id).array(), bytes -> ByteBuffer.wrap(bytes).getLong());

    // 实体 ID 和二进制之间的转换, 编码结果不超过 64 字节.
    byte @NotNull [] encode(@NotNull K id);

    @NotNull
    K decode(byte @NotNull [] bytes);

    @NotNull
    private static <K> EntityIdCodec<K> of(@NotNull Function<K, byte[]> encoder, @NotNull Function<byte[], K> decoder) {
        return new EntityIdCodec<>() {
            @Override
            public byte @NotNull [] encode(@NotNull K id) {
                return encoder.apply(id);
            }

            @NotNull
            @Override
            public K decode(byte @NotNull [] bytes) {
                return decoder.apply(bytes);
            }
        };
    }

    private static byte @NotNull [] encodeUuid(@NotNull UUID id) {
        return ByteBuffer.allocate(16)
                .putLong(id.getMostSignificantBits())
                .putLong(id.getLeastSignificantBits())
                .array();
    }

    @NotNull
    private static UUID decodeUuid(byte @NotNull [] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        // 构造参数从左到右求值, 先读出的是高 64 位
        return new UUID(buffer.getLong(), buffer.getLong());
    }
}