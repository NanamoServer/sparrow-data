package net.momirealms.sparrow.data.internal.key.builder;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.internal.key.MemoryDoubleKeyImpl;
import net.momirealms.sparrow.data.internal.key.ValueForm;
import net.momirealms.sparrow.data.key.MemoryDoubleKey;
import net.momirealms.sparrow.data.key.builder.MemoryDoubleKeyBuilder;
import net.momirealms.sparrow.data.key.builder.SortableMemoryDoubleKeyBuilder;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

@ApiStatus.Internal
public final class MemoryDoubleKeyBuilderImpl<K> implements MemoryDoubleKeyBuilder<K> {
    private final EntityTypeImpl<K> type;
    private final Key id;
    private double defaultValue;

    public MemoryDoubleKeyBuilderImpl(@NotNull EntityTypeImpl<K> type, @NotNull Key id) {
        this.type = type;
        this.id = id;
    }

    @NotNull
    @Override
    public MemoryDoubleKeyBuilder<K> defaultValue(double value) {
        this.defaultValue = value;
        return this;
    }

    @NotNull
    @Override
    public SortableMemoryDoubleKeyBuilder<K> sortable() {
        return new SortableMemoryDoubleKeyBuilderImpl<>(this.type, this.id, this.defaultValue);
    }

    @NotNull
    @Override
    public CompletableFuture<MemoryDoubleKey<K>> register() {
        MemoryDoubleKeyImpl<K> key = new MemoryDoubleKeyImpl<>(this.id, this.type, new ValueForm.DoubleForm(this.defaultValue));
        return this.type.backfill().register(key).<MemoryDoubleKey<K>>thenApply(ignored -> key);
    }
}