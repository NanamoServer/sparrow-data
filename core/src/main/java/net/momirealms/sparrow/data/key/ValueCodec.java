package net.momirealms.sparrow.data.key;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

public interface ValueCodec<T> {

    // 键的值和字节之间的转换, 内置的常用类型在 ValueCodecs 里. 同一个值每次都要编出同样的字节, decode(encode(value)) 要和 value 相等.
    // encode 返回的数组交给数据层后不要再改; decode 收到的数组数据层还要拿来做 CAS 比较, 返回的值不能直接引用它.
    // 字节不对时 decode 抛异常
    byte @NotNull [] encode(@NotNull T value);

    @NotNull
    T decode(byte @NotNull [] bytes);

    @NotNull
    default <R> ValueCodec<R> xmap(@NotNull Function<? super T, ? extends R> to, @NotNull Function<? super R, ? extends T> from) {
        return of(value -> this.encode(from.apply(value)), bytes -> to.apply(this.decode(bytes)));
    }

    @NotNull
    static <T> ValueCodec<T> of(@NotNull Function<? super T, byte[]> encoder, @NotNull Function<byte[], ? extends T> decoder) {
        return new ValueCodec<>() {
            @Override
            public byte @NotNull [] encode(@NotNull T value) {
                return encoder.apply(value);
            }

            @NotNull
            @Override
            public T decode(byte @NotNull [] bytes) {
                return decoder.apply(bytes);
            }
        };
    }

    // 先写元素个数, 再依次写每个元素. 解码出来的 List 不可修改
    @NotNull
    static <T> ValueCodec<List<T>> list(@NotNull ValueCodec<T> element) {
        return of(
                values -> {
                    ValueBytes.Output out = new ValueBytes.Output();
                    out.writeVarInt(values.size());
                    for (T value : values) {
                        out.write(element, value);
                    }
                    return out.toByteArray();
                },
                bytes -> {
                    ValueBytes.Input in = new ValueBytes.Input(bytes);
                    int count = in.readCount();
                    List<T> values = new ArrayList<>(count);
                    for (int i = 0; i < count; i++) {
                        values.add(in.read(element));
                    }
                    in.requireEnd();
                    return Collections.unmodifiableList(values);
                }
        );
    }

    // 先写条目个数, 再按迭代顺序依次写每个条目的键和值. 内容相同的两个 HashMap 迭代顺序可能不同, 需要稳定的字节就用 LinkedHashMap 或 TreeMap.
    // 解码出来的 Map 按编码顺序迭代, 不可修改
    @NotNull
    static <K, V> ValueCodec<Map<K, V>> map(@NotNull ValueCodec<K> key, @NotNull ValueCodec<V> value) {
        return of(
                entries -> {
                    ValueBytes.Output out = new ValueBytes.Output();
                    out.writeVarInt(entries.size());
                    for (Map.Entry<K, V> entry : entries.entrySet()) {
                        out.write(key, entry.getKey());
                        out.write(value, entry.getValue());
                    }
                    return out.toByteArray();
                },
                bytes -> {
                    ValueBytes.Input in = new ValueBytes.Input(bytes);
                    int count = in.readCount();
                    Map<K, V> entries = LinkedHashMap.newLinkedHashMap(count);
                    for (int i = 0; i < count; i++) {
                        K entryKey = in.read(key);
                        V entryValue = in.read(value);
                        entries.put(entryKey, entryValue);
                    }
                    in.requireEnd();
                    return Collections.unmodifiableMap(entries);
                }
        );
    }

    // 第一个字节 0 表示空, 1 表示有值, 有值时后面接内层的字节
    @NotNull
    static <T> ValueCodec<Optional<T>> optional(@NotNull ValueCodec<T> value) {
        return of(
                wrapped -> wrapped.isPresent() ? ValueBytes.present(value.encode(wrapped.get())) : ValueBytes.absent(),
                bytes -> {
                    byte[] inner = ValueBytes.presentValue(bytes);
                    return inner == null ? Optional.empty() : Optional.of(value.decode(inner));
                }
        );
    }

    // 字段按写入的顺序存放, 每个字段前面带 varint 长度. 读完需要的字段后剩下的数据忽略,
    // 所以新字段只能加在末尾, 读的一方用 Reader.hasRemaining() 判断旧数据里有没有它
    @NotNull
    static <T> ValueCodec<T> struct(@NotNull BiConsumer<? super T, Writer> writer, @NotNull Function<Reader, ? extends T> reader) {
        return of(
                value -> {
                    ValueBytes.Output out = new ValueBytes.Output();
                    writer.accept(value, out);
                    return out.toByteArray();
                },
                bytes -> reader.apply(new ValueBytes.Input(bytes))
        );
    }

    interface Writer {

        <V> void write(@NotNull ValueCodec<V> codec, @NotNull V value);
    }

    interface Reader {

        // 后面没有字段时抛 IllegalArgumentException
        @NotNull
        <V> V read(@NotNull ValueCodec<V> codec);

        boolean hasRemaining();
    }
}