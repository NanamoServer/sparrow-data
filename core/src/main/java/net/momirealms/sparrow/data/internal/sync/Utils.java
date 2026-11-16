package net.momirealms.sparrow.data.internal.sync;

import io.netty.buffer.ByteBuf;
import net.momirealms.sparrow.message.util.ByteBufHelper;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

final class Utils {

    // 可以为空的字段先写一个 boolean 表示有没有值, 有值时再按 ByteBufHelper 的格式写内容
    public static void writeNullableUtf8(@NotNull ByteBuf buf, @Nullable String text, int maxLength) {
        buf.writeBoolean(text != null);
        if (text != null) {
            ByteBufHelper.writeUtf8(buf, text, maxLength);
        }
    }

    @Nullable
    public static String readNullableUtf8(@NotNull ByteBuf buf, int maxLength) {
        return buf.readBoolean() ? ByteBufHelper.readUtf8(buf, maxLength) : null;
    }

    public static void writeNullableByteArray(@NotNull ByteBuf buf, byte @Nullable [] bytes) {
        buf.writeBoolean(bytes != null);
        if (bytes != null) {
            ByteBufHelper.writeByteArray(buf, bytes);
        }
    }

    public static byte @Nullable [] readNullableByteArray(@NotNull ByteBuf buf, int maxSize) {
        return buf.readBoolean() ? ByteBufHelper.readByteArray(buf, maxSize) : null;
    }
}