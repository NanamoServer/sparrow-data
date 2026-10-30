package net.momirealms.sparrow.data.key;

import org.jetbrains.annotations.NotNull;

import java.util.UUID;
import java.util.function.Function;

public interface EntryKeyCodec<E> {
    EntryKeyCodec<UUID> UUID = of(java.util.UUID::toString, java.util.UUID::fromString);
    EntryKeyCodec<String> STRING = of(Function.identity(), Function.identity());
    EntryKeyCodec<Integer> INT = of(String::valueOf, Integer::parseInt);
    EntryKeyCodec<Long> LONG = of(String::valueOf, Long::parseLong);

    // 条目键和字符串之间的转换, 字符串用来在存储里定位条目.
    @NotNull
    String encode(@NotNull E entry);

    @NotNull
    E decode(@NotNull String encoded);

    @NotNull
    private static <E> EntryKeyCodec<E> of(@NotNull Function<E, String> encoder, @NotNull Function<String, E> decoder) {
        return new EntryKeyCodec<>() {
            @NotNull
            @Override
            public String encode(@NotNull E entry) {
                return encoder.apply(entry);
            }

            @NotNull
            @Override
            public E decode(@NotNull String encoded) {
                return decoder.apply(encoded);
            }
        };
    }
}