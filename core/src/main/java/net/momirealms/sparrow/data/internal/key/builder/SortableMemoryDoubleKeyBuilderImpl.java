package net.momirealms.sparrow.data.internal.key.builder;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.internal.key.SortableMemoryDoubleKeyImpl;
import net.momirealms.sparrow.data.internal.key.ValueForm;
import net.momirealms.sparrow.data.key.SortableMemoryDoubleKey;
import net.momirealms.sparrow.data.key.builder.SortableMemoryDoubleKeyBuilder;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

@ApiStatus.Internal
public final class SortableMemoryDoubleKeyBuilderImpl<K> implements SortableMemoryDoubleKeyBuilder<K> {
    private final EntityTypeImpl<K> type;
    private final Key id;
    private double defaultValue;

    public SortableMemoryDoubleKeyBuilderImpl(@NotNull EntityTypeImpl<K> type, @NotNull Key id, double defaultValue) {
        this.type = type;
        this.id = id;
        this.defaultValue = defaultValue;
    }

    @NotNull
    @Override
    public SortableMemoryDoubleKeyBuilder<K> defaultValue(double value) {
        this.defaultValue = value;
        return this;
    }

    @NotNull
    @Override
    public CompletableFuture<SortableMemoryDoubleKey<K>> register() {
        SortableMemoryDoubleKeyImpl<K> key = new SortableMemoryDoubleKeyImpl<>(this.id, this.type, new ValueForm.DoubleForm(this.defaultValue));
        return this.type.backfill().register(key).<SortableMemoryDoubleKey<K>>thenApply(ignored -> key);
    }
}