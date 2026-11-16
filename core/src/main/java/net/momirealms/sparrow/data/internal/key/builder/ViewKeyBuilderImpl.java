package net.momirealms.sparrow.data.internal.key.builder;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.internal.key.ViewKeyImpl;
import net.momirealms.sparrow.data.key.ViewKey;
import net.momirealms.sparrow.data.key.builder.ViewKeyBuilder;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

@ApiStatus.Internal
public final class ViewKeyBuilderImpl<K, T> implements ViewKeyBuilder<K, T> {
    private final EntityTypeImpl<K> type;
    private final Key id;
    private final Function<K, CompletableFuture<T>> loader;

    public ViewKeyBuilderImpl(@NotNull EntityTypeImpl<K> type, @NotNull Key id, @NotNull Function<K, CompletableFuture<T>> loader) {
        this.type = type;
        this.id = id;
        this.loader = loader;
    }

    @NotNull
    @Override
    public ViewKey<K, T> register() {
        ViewKeyImpl<K, T> key = new ViewKeyImpl<>(this.id, this.type, this.loader);
        this.type.keys().register(key);
        return key;
    }
}