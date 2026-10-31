package net.momirealms.sparrow.data.key;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

final class ValueBytes {

    private ValueBytes() {
    }

    // optional 组合的字节, 格式见 ValueCodec.optional
    static byte @NotNull [] present(byte @NotNull [] value) {
        byte[] bytes = new byte[value.length + 1];
        bytes[0] = 1;
        System.arraycopy(value, 0, bytes, 1, value.length);
        return bytes;
    }

    static byte @NotNull [] absent() {
        return new byte[]{0};
    }

    // 返回 null 表示空
    static byte @Nullable [] presentValue(byte @NotNull [] bytes) {
        if (bytes.length == 1 && bytes[0] == 0) return null;
        if (bytes.length == 0 || bytes[0] != 1) throw new IllegalArgumentException("Malformed optional value of " + bytes.length + " bytes");
        return Arrays.copyOfRange(bytes, 1, bytes.length);
    }

    // 组合出来的值的写入端. 长度和个数都写成无符号 varint, 每个字节存 7 位, 低位在前
    static final class Output implements ValueCodec.Writer {
        private byte[] buffer = new byte[64];
        private int size;

        @Override
        public <V> void write(@NotNull ValueCodec<V> codec, @NotNull V value) {
            byte[] encoded = codec.encode(value);
            this.writeVarInt(encoded.length);
            this.ensure(encoded.length);
            System.arraycopy(encoded, 0, this.buffer, this.size, encoded.length);
            this.size += encoded.length;
        }

        void writeVarInt(int value) {
            this.ensure(5);
            int remaining = value;
            while ((remaining & ~0x7F) != 0) {
                this.buffer[this.size++] = (byte) (remaining & 0x7F | 0x80);
                remaining >>>= 7;
            }
            this.buffer[this.size++] = (byte) remaining;
        }

        byte @NotNull [] toByteArray() {
            return Arrays.copyOf(this.buffer, this.size);
        }

        private void ensure(int extra) {
            int needed = this.size + extra;
            if (needed > this.buffer.length) {
                this.buffer = Arrays.copyOf(this.buffer, Math.max(needed, this.buffer.length * 2));
            }
        }
    }

    // 组合出来的值的读取端. 字节来自存储和消息, 长度和个数都先和剩下的字节数比较, 坏数据抛 IllegalArgumentException
    static final class Input implements ValueCodec.Reader {
        private final byte[] bytes;
        private int position;

        Input(byte @NotNull [] bytes) {
            this.bytes = bytes;
        }

        @NotNull
        @Override
        public <V> V read(@NotNull ValueCodec<V> codec) {
            if (!this.hasRemaining()) throw new IllegalArgumentException("No more fields to read");
            int length = this.readVarInt();
            if (length > this.remaining()) throw new IllegalArgumentException("Field of " + length + " bytes exceeds the remaining " + this.remaining() + " bytes");
            byte[] field = Arrays.copyOfRange(this.bytes, this.position, this.position + length);
            this.position += length;
            return codec.decode(field);
        }

        @Override
        public boolean hasRemaining() {
            return this.position < this.bytes.length;
        }

        // 每个元素至少占 1 个字节的长度, 个数超过剩下的字节数就是坏数据. 通过检查的个数可以直接用作集合的初始容量
        int readCount() {
            int count = this.readVarInt();
            if (count > this.remaining()) throw new IllegalArgumentException("Count " + count + " exceeds the remaining " + this.remaining() + " bytes");
            return count;
        }

        void requireEnd() {
            if (this.hasRemaining()) throw new IllegalArgumentException("Unexpected " + this.remaining() + " trailing bytes");
        }

        private int remaining() {
            return this.bytes.length - this.position;
        }

        private int readVarInt() {
            int value = 0;
            for (int shift = 0; shift < 35; shift += 7) {
                if (!this.hasRemaining()) throw new IllegalArgumentException("Truncated varint");
                byte current = this.bytes[this.position++];
                value |= (current & 0x7F) << shift;
                if ((current & 0x80) == 0) {
                    if (value < 0) throw new IllegalArgumentException("Varint is out of range");
                    return value;
                }
            }
            throw new IllegalArgumentException("Varint is longer than 5 bytes");
        }
    }
}