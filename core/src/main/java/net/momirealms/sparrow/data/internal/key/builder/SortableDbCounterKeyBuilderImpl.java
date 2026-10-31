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
    private int scale;

    public SortableDbCounterKeyBuilderImpl(@NotNull EntityTypeImpl<K> type, @NotNull Key id, int scale) {
        this.type = type;
        this.id = id;
        this.scale = scale;
    }

    @NotNull
    @Override
    public SortableDbCounterKeyBuilder<K> scale(int scale) {
        this.scale = ScaleRange.require(scale);
        return this;
    }

    @NotNull
    @Override
    public SortableDbCounterKey<K> register() {
        SortableDbCounterKeyImpl<K> key = new SortableDbCounterKeyImpl<>(this.id, this.type, this.scale);
        this.type.keys().register(key);
        return key;
    }
}