package net.momirealms.sparrow.data.internal.key.builder;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.internal.key.SortableMemoryLongKeyImpl;
import net.momirealms.sparrow.data.internal.key.ValueForm;
import net.momirealms.sparrow.data.key.SortableMemoryLongKey;
import net.momirealms.sparrow.data.key.builder.SortableMemoryLongKeyBuilder;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

@ApiStatus.Internal
public final class SortableMemoryLongKeyBuilderImpl<K> implements SortableMemoryLongKeyBuilder<K> {
    private final EntityTypeImpl<K> type;
    private final Key id;
    private long defaultValue;
    private int scale;

    public SortableMemoryLongKeyBuilderImpl(@NotNull EntityTypeImpl<K> type, @NotNull Key id, long defaultValue, int scale) {
        this.type = type;
        this.id = id;
        this.defaultValue = defaultValue;
        this.scale = scale;
    }

    @NotNull
    @Override
    public SortableMemoryLongKeyBuilder<K> defaultValue(long value) {
        this.defaultValue = value;
        return this;
    }

    @NotNull
    @Override
    public SortableMemoryLongKeyBuilder<K> scale(int scale) {
        this.scale = ScaleRange.require(scale);
        return this;
    }

    @NotNull
    @Override
    public CompletableFuture<SortableMemoryLongKey<K>> register() {
        SortableMemoryLongKeyImpl<K> key = new SortableMemoryLongKeyImpl<>(this.id, this.type, new ValueForm.LongForm(this.defaultValue), this.scale);
        return this.type.backfill().register(key).thenApply(ignored -> key);
    }
}