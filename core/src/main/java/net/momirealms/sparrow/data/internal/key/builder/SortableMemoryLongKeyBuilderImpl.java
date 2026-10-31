package net.momirealms.sparrow.data.internal.key.builder;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.internal.key.SortableMemoryKeyImpl;
import net.momirealms.sparrow.data.internal.key.ValueForm;
import net.momirealms.sparrow.data.key.SortableMemoryKey;
import net.momirealms.sparrow.data.key.builder.SortableMemoryLongKeyBuilder;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

@ApiStatus.Internal
public final class SortableMemoryLongKeyBuilderImpl<K> implements SortableMemoryLongKeyBuilder<K> {
    private final EntityTypeImpl<K> type;
    private final Key id;
    private long defaultValue;

    public SortableMemoryLongKeyBuilderImpl(@NotNull EntityTypeImpl<K> type, @NotNull Key id, long defaultValue) {
        this.type = type;
        this.id = id;
        this.defaultValue = defaultValue;
    }

    @NotNull
    @Override
    public SortableMemoryLongKeyBuilder<K> defaultValue(long value) {
        this.defaultValue = value;
        return this;
    }

    @NotNull
    @Override
    public CompletableFuture<SortableMemoryKey<K>> register() {
        SortableMemoryKeyImpl<K> key = new SortableMemoryKeyImpl<>(this.id, this.type, new ValueForm.LongForm(this.defaultValue));
        return this.type.backfill().register(key).<SortableMemoryKey<K>>thenApply(ignored -> key);
    }
}