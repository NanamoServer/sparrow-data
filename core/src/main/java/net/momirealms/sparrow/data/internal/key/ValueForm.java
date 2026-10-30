package net.momirealms.sparrow.data.internal.key;

import com.mojang.serialization.Codec;
import net.momirealms.sparrow.data.internal.codec.NbtValues;
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

    // 副本里的值统一是 NBT 二进制, long 值也编码成 NBT. 返回 null 表示发布的是"没有值", 副本里存空值
    byte @Nullable [] replica(@NotNull T value);

    // replica 为空表示持有者发布的是"没有值"
    @Nullable
    T fromReplica(byte @NotNull [] replica);

    // 用 Codec 编成 NBT 字节. 存储里没有值时读到 fallback, 映射键的条目没有 fallback
    record NbtForm<T>(@NotNull Codec<T> codec, @Nullable T fallback) implements ValueForm<T> {

        @NotNull
        @Override
        public StoredValue store(@NotNull T value) {
            return new StoredValue.OfBytes(NbtValues.encode(this.codec, value));
        }

        @Nullable
        @Override
        public T load(@Nullable StoredValue stored) {
            if (stored == null) return this.fallback;
            return NbtValues.decode(this.codec, ((StoredValue.OfBytes) stored).value());
        }

        @Override
        public byte @NotNull [] replica(@NotNull T value) {
            return NbtValues.encode(this.codec, value);
        }

        @Nullable
        @Override
        public T fromReplica(byte @NotNull [] replica) {
            if (replica.length == 0) return this.fallback;
            return NbtValues.decode(this.codec, replica);
        }
    }

    // 不设默认值的键. 存储里只放 Optional 里面的值, Optional.empty() 对应存储里没有值
    record OptionalNbtForm<T>(@NotNull Codec<T> codec) implements ValueForm<Optional<T>> {

        @Nullable
        @Override
        public StoredValue store(@NotNull Optional<T> value) {
            if (value.isEmpty()) return null;
            return new StoredValue.OfBytes(NbtValues.encode(this.codec, value.get()));
        }

        @NotNull
        @Override
        public Optional<T> load(@Nullable StoredValue stored) {
            if (stored == null) return Optional.empty();
            return Optional.of(NbtValues.decode(this.codec, ((StoredValue.OfBytes) stored).value()));
        }

        @Override
        public byte @Nullable [] replica(@NotNull Optional<T> value) {
            if (value.isEmpty()) return null;
            return NbtValues.encode(this.codec, value.get());
        }

        @NotNull
        @Override
        public Optional<T> fromReplica(byte @NotNull [] replica) {
            if (replica.length == 0) return Optional.empty();
            return Optional.of(NbtValues.decode(this.codec, replica));
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

        @Override
        public byte @NotNull [] replica(@NotNull Long value) {
            return NbtValues.encode(Codec.LONG, value);
        }

        @NotNull
        @Override
        public Long fromReplica(byte @NotNull [] replica) {
            return NbtValues.decode(Codec.LONG, replica);
        }
    }
}