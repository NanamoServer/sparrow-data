package net.momirealms.sparrow.data.internal.key.builder;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.internal.key.MemoryMapKeyImpl;
import net.momirealms.sparrow.data.internal.key.ValueForm;
import net.momirealms.sparrow.data.key.EntryKeyCodec;
import net.momirealms.sparrow.data.key.MemoryMapKey;
import net.momirealms.sparrow.data.key.builder.MemoryMapKeyBuilder;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

@ApiStatus.Internal
public final class MemoryMapKeyBuilderImpl<K, E, V> implements MemoryMapKeyBuilder<K, E, V> {
    private final EntityTypeImpl<K> type;
    private final Key id;
    private final EntryKeyCodec<E> entryCodec;
    private final ValueForm<V> valueForm;

    public MemoryMapKeyBuilderImpl(@NotNull EntityTypeImpl<K> type, @NotNull Key id, @NotNull EntryKeyCodec<E> entryCodec, @NotNull ValueForm<V> valueForm) {
        this.type = type;
        this.id = id;
        this.entryCodec = entryCodec;
        this.valueForm = valueForm;
    }

    @NotNull
    @Override
    public CompletableFuture<MemoryMapKey<K, E, V>> register() {
        MemoryMapKeyImpl<K, E, V> key = new MemoryMapKeyImpl<>(this.id, this.type, this.entryCodec, this.valueForm);
        return this.type.backfill().register(key).<MemoryMapKey<K, E, V>>thenApply(ignored -> key);
    }
}