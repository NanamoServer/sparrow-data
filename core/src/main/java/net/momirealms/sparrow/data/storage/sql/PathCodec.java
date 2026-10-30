package net.momirealms.sparrow.data.storage.sql;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@ApiStatus.Internal
public final class PathCodec {
    private static final int MAX_PATH_LENGTH = 255;

    private PathCodec() {
    }

    // 普通键的 path 就是键 ID; 条目的 path 是键 ID + "." + 转义后的条目键. 键 ID 不含 ".", 所以 path 里只有一个真正的分隔符
    @NotNull
    public static String path(@NotNull String keyId, @Nullable String entry) {
        if (entry == null) return keyId;
        String path = keyId + '.' + escape(entry);
        if (path.length() > MAX_PATH_LENGTH) throw new IllegalArgumentException("Entry path is longer than " + MAX_PATH_LENGTH + " characters: " + path);
        return path;
    }

    // 一个键全部条目的范围是 [键ID., 键ID/). "." 是 0x2E, "/" 是 0x2F, 按字节比较时紧挨着, 这个半开区间正好是所有以 "键ID." 开头的 path
    @NotNull
    public static String entriesFrom(@NotNull String keyId) {
        return keyId + '.';
    }

    @NotNull
    public static String entriesTo(@NotNull String keyId) {
        return keyId + '/';
    }

    @NotNull
    public static String keyId(@NotNull String path) {
        int separator = path.indexOf('.');
        return separator < 0 ? path : path.substring(0, separator);
    }

    @Nullable
    public static String entry(@NotNull String path) {
        int separator = path.indexOf('.');
        return separator < 0 ? null : unescape(path.substring(separator + 1));
    }

    @NotNull
    private static String escape(@NotNull String entry) {
        StringBuilder builder = new StringBuilder(entry.length());
        int length = entry.length();
        for (int i = 0; i < length; i++) {
            char c = entry.charAt(i);
            switch (c) {
                case '%' -> builder.append("%25");
                case '.' -> builder.append("%2E");
                default -> builder.append(c);
            }
        }
        return builder.toString();
    }

    // 只会遇到 escape 产生的 %25 和 %2E, 两位十六进制直接还原成一个字符
    @NotNull
    private static String unescape(@NotNull String escaped) {
        if (escaped.indexOf('%') < 0) return escaped;
        StringBuilder builder = new StringBuilder(escaped.length());
        int length = escaped.length();
        for (int i = 0; i < length; i++) {
            char c = escaped.charAt(i);
            if (c == '%') {
                builder.append((char) Integer.parseInt(escaped, i + 1, i + 3, 16));
                i += 2;
            } else {
                builder.append(c);
            }
        }
        return builder.toString();
    }
}