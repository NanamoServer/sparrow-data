package net.momirealms.sparrow.data.internal.key;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.key.ViewKey;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

@ApiStatus.Internal
public final class ViewKeyImpl<K, T> extends AbstractDataKey<K> implements ViewKey<K, T> {
    private final Function<K, CompletableFuture<T>> loader;

    public ViewKeyImpl(@NotNull Key id, @NotNull EntityTypeImpl<K> type, @NotNull Function<K, CompletableFuture<T>> loader) {
        super(id, type, false);
        this.loader = loader;
    }

    // loader 来自宿主. 快照要求读取函数只通过 future 报告失败, 它直接抛出的异常在这里转成失败的 future
    @NotNull
    public CompletableFuture<T> load(@NotNull K id) {
        try {
            return this.loader.apply(id);
        } catch (RuntimeException exception) {
            return CompletableFuture.failedFuture(exception);
        }
    }
}