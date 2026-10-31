package net.momirealms.sparrow.data.internal.key.builder;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.internal.key.DbCounterKeyImpl;
import net.momirealms.sparrow.data.key.DbCounterKey;
import net.momirealms.sparrow.data.key.builder.DbCounterKeyBuilder;
import net.momirealms.sparrow.data.key.builder.SortableDbCounterKeyBuilder;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public final class DbCounterKeyBuilderImpl<K> implements DbCounterKeyBuilder<K> {
    private final EntityTypeImpl<K> type;
    private final Key id;
    private int scale;

    public DbCounterKeyBuilderImpl(@NotNull EntityTypeImpl<K> type, @NotNull Key id) {
        this.type = type;
        this.id = id;
    }

    @NotNull
    @Override
    public DbCounterKeyBuilder<K> scale(int scale) {
        this.scale = ScaleRange.require(scale);
        return this;
    }

    @NotNull
    @Override
    public SortableDbCounterKeyBuilder<K> sortable() {
        return new SortableDbCounterKeyBuilderImpl<>(this.type, this.id, this.scale);
    }

    @NotNull
    @Override
    public DbCounterKey<K> register() {
        DbCounterKeyImpl<K> key = new DbCounterKeyImpl<>(this.id, this.type, this.scale);
        this.type.keys().register(key);
        return key;
    }
}