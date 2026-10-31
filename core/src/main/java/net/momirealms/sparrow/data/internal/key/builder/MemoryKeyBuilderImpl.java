package net.momirealms.sparrow.data.internal.key.builder;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.internal.key.MemoryKeyImpl;
import net.momirealms.sparrow.data.internal.key.ValueForm;
import net.momirealms.sparrow.data.key.MemoryKey;
import net.momirealms.sparrow.data.key.builder.MemoryKeyBuilder;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

@ApiStatus.Internal
public final class MemoryKeyBuilderImpl<K, T> implements MemoryKeyBuilder<K, T> {
    private final EntityTypeImpl<K> type;
    private final Key id;
    private final ValueForm<T> form;

    public MemoryKeyBuilderImpl(@NotNull EntityTypeImpl<K> type, @NotNull Key id, @NotNull ValueForm<T> form) {
        this.type = type;
        this.id = id;
        this.form = form;
    }

    @NotNull
    @Override
    public CompletableFuture<MemoryKey<K, T>> register() {
        MemoryKeyImpl<K, T> key = new MemoryKeyImpl<>(this.id, this.type, this.form);
        return this.type.backfill().register(key).<MemoryKey<K, T>>thenApply(ignored -> key);
    }
}