package net.momirealms.sparrow.data.internal.key.builder;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.internal.key.SortableDbCounterKeyImpl;
import net.momirealms.sparrow.data.key.SortableDbCounterKey;
import net.momirealms.sparrow.data.key.builder.SortableDbCounterKeyBuilder;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public final class SortableDbCounterKeyBuilderImpl<K> implements SortableDbCounterKeyBuilder<K> {
    private final EntityTypeImpl<K> type;
    private final Key id;

    public SortableDbCounterKeyBuilderImpl(@NotNull EntityTypeImpl<K> type, @NotNull Key id) {
        this.type = type;
        this.id = id;
    }

    @NotNull
    @Override
    public SortableDbCounterKey<K> register() {
        SortableDbCounterKeyImpl<K> key = new SortableDbCounterKeyImpl<>(this.id, this.type);
        this.type.keys().register(key);
        return key;
    }
}