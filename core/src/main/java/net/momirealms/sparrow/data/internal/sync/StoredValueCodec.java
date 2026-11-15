package net.momirealms.sparrow.data.internal.sync;

import io.netty.buffer.ByteBuf;
import net.momirealms.sparrow.data.storage.StoredValue;
import net.momirealms.sparrow.message.util.ByteBufHelper;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@ApiStatus.Internal
final class StoredValueCodec {
    private static final byte NO_VALUE = 0;
    private static final byte LONG_VALUE = 1;
    private static final byte BYTES_VALUE = 2;

    private StoredValueCodec() {
    }

    // 覆盖和 publish 消息里的值. 先写一个字节表示形态, 再写内容, null 表示没有值
    static void write(@NotNull ByteBuf buf, @Nullable StoredValue value) {
        switch (value) {
            case null -> buf.writeByte(NO_VALUE);
            case StoredValue.OfLong number -> {
                buf.writeByte(LONG_VALUE);
                buf.writeLong(number.value());
            }
            case StoredValue.OfBytes bytes -> {
                buf.writeByte(BYTES_VALUE);
                ByteBufHelper.writeByteArray(buf, bytes.value());
            }
        }
    }

    @Nullable
    static StoredValue read(@NotNull ByteBuf buf) {
        return switch (buf.readByte()) {
            case LONG_VALUE -> new StoredValue.OfLong(buf.readLong());
            case BYTES_VALUE -> new StoredValue.OfBytes(ByteBufHelper.readByteArray(buf, Integer.MAX_VALUE));
            default -> null;
        };
    }
}