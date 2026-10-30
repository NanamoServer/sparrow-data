package net.momirealms.sparrow.data.internal.codec;

import com.mojang.serialization.Codec;
import net.momirealms.sparrow.nbt.NBT;
import net.momirealms.sparrow.nbt.Tag;
import net.momirealms.sparrow.nbt.codec.NBTOps;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

@ApiStatus.Internal
public final class NbtValues {

    private NbtValues() {
    }

    // 用 sparrow-nbt 在 {@code Codec<T>} 和 NBT 二进制之间转换.
    public static <T> byte @NotNull [] encode(@NotNull Codec<T> codec, @NotNull T value) {
        Tag tag = codec.encodeStart(NBTOps.INSTANCE, value).getOrThrow();
        try {
            return NBT.toBytes(tag, false);
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    @NotNull
    public static <T> T decode(@NotNull Codec<T> codec, byte @NotNull [] bytes) {
        Tag tag;
        try {
            // 值不一定是 compound, 所以按任意 Tag 读取, 和写入时一样不带根标签名
            tag = NBT.readUnnamedTag(new DataInputStream(new ByteArrayInputStream(bytes)), false);
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
        return codec.parse(NBTOps.INSTANCE, tag).getOrThrow();
    }
}