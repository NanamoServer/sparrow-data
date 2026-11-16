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

    // int, long, double 三种数值键. 持有容器里这种键的值放在一个 long 里, 读写不装箱: int 和 long 就是数值本身, double 是它的位模式.
    // 装箱的 T 只在通用的读写路径上出现, 例如覆盖, 快照和 watch
    sealed interface NumberForm<T> extends ValueForm<T> {

        long fallbackBits();

        long toBits(@NotNull T value);

        @NotNull
        T fromBits(long bits);

        @NotNull
        StoredValue storeBits(long bits);

        // stored 为 null 时是默认值
        long loadBits(@Nullable StoredValue stored);

        @Nullable
        @Override
        default StoredValue store(@Nullable T value) {
            return value == null ? null : this.storeBits(this.toBits(value));
        }

        @NotNull
        @Override
        default T load(@Nullable StoredValue stored) {
            return this.fromBits(this.loadBits(stored));
        }
    }

    // memoryInt 键, 存储里和 long 一样放在 num 列
    record IntForm(int fallback) implements NumberForm<Integer> {

        @Override
        public long fallbackBits() {
            return this.fallback;
        }

        @Override
        public long toBits(@NotNull Integer value) {
            return value;
        }

        @NotNull
        @Override
        public Integer fromBits(long bits) {
            return (int) bits;
        }

        @NotNull
        @Override
        public StoredValue storeBits(long bits) {
            return new StoredValue.OfLong(bits);
        }

        @Override
        public long loadBits(@Nullable StoredValue stored) {
            return stored == null ? this.fallback : ((StoredValue.OfLong) stored).value();
        }
    }

    // memoryLong 键和计数键
    record LongForm(long fallback) implements NumberForm<Long> {
        public static final LongForm ZERO = new LongForm(0L);

        @Override
        public long fallbackBits() {
            return this.fallback;
        }

        @Override
        public long toBits(@NotNull Long value) {
            return value;
        }

        @NotNull
        @Override
        public Long fromBits(long bits) {
            return bits;
        }

        @NotNull
        @Override
        public StoredValue storeBits(long bits) {
            return new StoredValue.OfLong(bits);
        }

        @Override
        public long loadBits(@Nullable StoredValue stored) {
            return stored == null ? this.fallback : ((StoredValue.OfLong) stored).value();
        }
    }

    // memoryDouble 键, 存储里放在 dbl 列
    record DoubleForm(double fallback) implements NumberForm<Double> {

        @Override
        public long fallbackBits() {
            return Double.doubleToRawLongBits(this.fallback);
        }

        @Override
        public long toBits(@NotNull Double value) {
            return Double.doubleToRawLongBits(value);
        }

        @NotNull
        @Override
        public Double fromBits(long bits) {
            return Double.longBitsToDouble(bits);
        }

        @NotNull
        @Override
        public StoredValue storeBits(long bits) {
            return new StoredValue.OfDouble(Double.longBitsToDouble(bits));
        }

        @Override
        public long loadBits(@Nullable StoredValue stored) {
            return Double.doubleToRawLongBits(stored == null ? this.fallback : ((StoredValue.OfDouble) stored).value());
        }
    }
}