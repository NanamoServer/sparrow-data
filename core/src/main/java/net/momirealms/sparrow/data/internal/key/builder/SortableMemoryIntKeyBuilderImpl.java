package net.momirealms.sparrow.data.internal.key.builder;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.internal.key.SortableMemoryIntKeyImpl;
import net.momirealms.sparrow.data.internal.key.ValueForm;
import net.momirealms.sparrow.data.key.SortableMemoryIntKey;
import net.momirealms.sparrow.data.key.builder.SortableMemoryIntKeyBuilder;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

@ApiStatus.Internal
public final class SortableMemoryIntKeyBuilderImpl<K> implements SortableMemoryIntKeyBuilder<K> {
    private final EntityTypeImpl<K> type;
    private final Key id;
    private int defaultValue;

    public SortableMemoryIntKeyBuilderImpl(@NotNull EntityTypeImpl<K> type, @NotNull Key id, int defaultValue) {
        this.type = type;
        this.id = id;
        this.defaultValue = defaultValue;
    }

    @NotNull
    @Override
    public SortableMemoryIntKeyBuilder<K> defaultValue(int value) {
        this.defaultValue = value;
        return this;
    }

    @NotNull
    @Override
    public CompletableFuture<SortableMemoryIntKey<K>> register() {
        SortableMemoryIntKeyImpl<K> key = new SortableMemoryIntKeyImpl<>(this.id, this.type, new ValueForm.IntForm(this.defaultValue));
        return this.type.backfill().register(key).<SortableMemoryIntKey<K>>thenApply(ignored -> key);
    }
}