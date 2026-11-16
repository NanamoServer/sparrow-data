package net.momirealms.sparrow.data.internal.key;

import net.momirealms.sparrow.data.key.ValueCodec;
import net.momirealms.sparrow.data.storage.StoredValue;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;

@ApiStatus.Internal
public sealed interface ValueForm<T> {

    // 键的值和存储里的值之间的转换. 返回 null 表示没有要存的值, 写入时要删除存储里的值
    @Nullable
    StoredValue store(@NotNull T value);

    // stored 为 null 表示存储里没有值
    @Nullable
    T load(@Nullable StoredValue stored);

    // 用 ValueCodec 编成字节. 存储里没有值时读到 fallback
    record CodecForm<T>(@NotNull ValueCodec<T> codec, @NotNull T fallback) implements ValueForm<T> {

        @NotNull
        @Override
        public StoredValue store(@NotNull T value) {
            return new StoredValue.OfBytes(this.codec.encode(value));
        }

        @Nullable
        @Override
        public T load(@Nullable StoredValue stored) {
            if (stored == null) return this.fallback;
            return this.codec.decode(((StoredValue.OfBytes) stored).value());
        }
    }

    // 不设默认值的键. 存储里只放 Optional 里面的值, Optional.empty() 对应存储里没有值
    record OptionalCodecForm<T>(@NotNull ValueCodec<T> codec) implements ValueForm<Optional<T>> {

        @Nullable
        @Override
        public StoredValue store(@NotNull Optional<T> value) {
            if (value.isEmpty()) return null;
            return new StoredValue.OfBytes(this.codec.encode(value.get()));
        }

        @NotNull
        @Override
        public Optional<T> load(@Nullable StoredValue stored) {
            if (stored == null) return Optional.empty();
            return Optional.of(this.codec.decode(((StoredValue.OfBytes) stored).value()));
        }
    }

    // memoryLong 键和计数键, 值直接以 long 存储
    record LongForm(long fallback) implements ValueForm<Long> {
        public static final LongForm ZERO = new LongForm(0L);

        @NotNull
        @Override
        public StoredValue store(@NotNull Long value) {
            return new StoredValue.OfLong(value);
        }

        @NotNull
        @Override
        public Long load(@Nullable StoredValue stored) {
            if (stored == null) return this.fallback;
            return ((StoredValue.OfLong) stored).value();
        }
    }
}