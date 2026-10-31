package net.momirealms.sparrow.data;

import org.jetbrains.annotations.NotNull;

/** 键 ID, 由命名空间和值组成, 写作 {@code 命名空间:值}. */
public record Key(@NotNull String namespace, @NotNull String value) {
    public static final String DEFAULT_NAMESPACE = "sparrow";

    @NotNull
    public static Key sparrow(@NotNull String value) {
        return new Key(DEFAULT_NAMESPACE, value);
    }

    // 解析 "命名空间:值", 没有写命名空间时使用 sparrow
    @NotNull
    public static Key of(@NotNull String id) {
        int separator = id.indexOf(':');
        return separator < 0 ? sparrow(id) : new Key(id.substring(0, separator), id.substring(separator + 1));
    }

    // 命名空间是 sparrow 时只写值
    @NotNull
    public String asMinimalString() {
        return this.namespace.equals(DEFAULT_NAMESPACE) ? this.value : this.toString();
    }

    @NotNull
    @Override
    public String toString() {
        return this.namespace + ":" + this.value;
    }
}