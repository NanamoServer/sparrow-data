package net.momirealms.sparrow.data.internal.key.builder;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.internal.key.MemoryLongKeyImpl;
import net.momirealms.sparrow.data.internal.key.ValueForm;
import net.momirealms.sparrow.data.key.MemoryLongKey;
import net.momirealms.sparrow.data.key.builder.MemoryLongKeyBuilder;
import net.momirealms.sparrow.data.key.builder.SortableMemoryLongKeyBuilder;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

@ApiStatus.Internal
public final class MemoryLongKeyBuilderImpl<K> implements MemoryLongKeyBuilder<K> {
    private final EntityTypeImpl<K> type;
    private final Key id;
    private long defaultValue;
    private int scale;

    public MemoryLongKeyBuilderImpl(@NotNull EntityTypeImpl<K> type, @NotNull Key id) {
        this.type = type;
        this.id = id;
    }

    @NotNull
    @Override
    public MemoryLongKeyBuilder<K> defaultValue(long value) {
        this.defaultValue = value;
        return this;
    }

    @NotNull
    @Override
    public MemoryLongKeyBuilder<K> scale(int scale) {
        this.scale = ScaleRange.require(scale);
        return this;
    }

    @NotNull
    @Override
    public SortableMemoryLongKeyBuilder<K> sortable() {
        return new SortableMemoryLongKeyBuilderImpl<>(this.type, this.id, this.defaultValue, this.scale);
    }

    @NotNull
    @Override
    public CompletableFuture<MemoryLongKey<K>> register() {
        MemoryLongKeyImpl<K> key = new MemoryLongKeyImpl<>(this.id, this.type, new ValueForm.LongForm(this.defaultValue), this.scale);
        return this.type.backfill().register(key).<MemoryLongKey<K>>thenApply(ignored -> key);
    }
}