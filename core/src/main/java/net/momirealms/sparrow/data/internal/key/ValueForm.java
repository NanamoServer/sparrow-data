package net.momirealms.sparrow.data.internal.key;

import net.momirealms.sparrow.data.key.ValueCodec;
import net.momirealms.sparrow.data.storage.StoredValue;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@ApiStatus.Internal
public sealed interface ValueForm<T> {

    // 键的值和存储里的值之间的转换. value 为 null 时返回 null, 表示存储里不该有值, 写入时要删除
    @Nullable
    StoredValue store(@Nullable T value);

    // stored 为 null 表示存储里没有值, 这时读到默认值, 没有默认值的键读到 null
    @Nullable
    T load(@Nullable StoredValue stored);

    // 调用方写入 null 之后, 读取方看到的值
    @Nullable
    default T orFallback(@Nullable T value) {
        return value == null ? this.load(null) : value;
    }

    // 用 ValueCodec 编成字节. fallback 是注册时给的默认值, 没给时为 null
    record CodecForm<T>(@NotNull ValueCodec<T> codec, @Nullable T fallback) implements ValueForm<T> {

        @Nullable
        @Override
        public StoredValue store(@Nullable T value) {
            return value == null ? null : new StoredValue.OfBytes(this.codec.encode(value));
        }

        @Nullable
        @Override
        public T load(@Nullable StoredValue stored) {
            if (stored == null) return this.fallback;
            return this.codec.decode(((StoredValue.OfBytes) stored).value());
        }
    }

    // memoryLong 键和计数键, 值直接以 long 存储
    record LongForm(long fallback) implements ValueForm<Long> {
        public static final LongForm ZERO = new LongForm(0L);

        @Nullable
        @Override
        public StoredValue store(@Nullable Long value) {
            return value == null ? null : new StoredValue.OfLong(value);
        }

        @NotNull
        @Override
        public Long load(@Nullable StoredValue stored) {
            if (stored == null) return this.fallback;
            return ((StoredValue.OfLong) stored).value();
        }
    }
}