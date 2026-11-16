package net.momirealms.sparrow.data.internal.key.builder;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.internal.key.MemoryIntKeyImpl;
import net.momirealms.sparrow.data.internal.key.ValueForm;
import net.momirealms.sparrow.data.key.MemoryIntKey;
import net.momirealms.sparrow.data.key.builder.MemoryIntKeyBuilder;
import net.momirealms.sparrow.data.key.builder.SortableMemoryIntKeyBuilder;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

@ApiStatus.Internal
public final class MemoryIntKeyBuilderImpl<K> implements MemoryIntKeyBuilder<K> {
    private final EntityTypeImpl<K> type;
    private final Key id;
    private int defaultValue;

    public MemoryIntKeyBuilderImpl(@NotNull EntityTypeImpl<K> type, @NotNull Key id) {
        this.type = type;
        this.id = id;
    }

    @NotNull
    @Override
    public MemoryIntKeyBuilder<K> defaultValue(int value) {
        this.defaultValue = value;
        return this;
    }

    @NotNull
    @Override
    public SortableMemoryIntKeyBuilder<K> sortable() {
        return new SortableMemoryIntKeyBuilderImpl<>(this.type, this.id, this.defaultValue);
    }

    @NotNull
    @Override
    public CompletableFuture<MemoryIntKey<K>> register() {
        MemoryIntKeyImpl<K> key = new MemoryIntKeyImpl<>(this.id, this.type, new ValueForm.IntForm(this.defaultValue));
        return this.type.backfill().register(key).<MemoryIntKey<K>>thenApply(ignored -> key);
    }
}