package net.momirealms.sparrow.data.internal.key.builder;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.internal.key.DbMapCounterKeyImpl;
import net.momirealms.sparrow.data.key.DbMapCounterKey;
import net.momirealms.sparrow.data.key.EntryKeyCodec;
import net.momirealms.sparrow.data.key.builder.DbMapCounterKeyBuilder;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public final class DbMapCounterKeyBuilderImpl<K, E> implements DbMapCounterKeyBuilder<K, E> {
    private final EntityTypeImpl<K> type;
    private final Key id;
    private final EntryKeyCodec<E> entryCodec;
    private int scale;

    public DbMapCounterKeyBuilderImpl(@NotNull EntityTypeImpl<K> type, @NotNull Key id, @NotNull EntryKeyCodec<E> entryCodec) {
        this.type = type;
        this.id = id;
        this.entryCodec = entryCodec;
    }

    @NotNull
    @Override
    public DbMapCounterKeyBuilder<K, E> scale(int scale) {
        this.scale = ScaleRange.require(scale);
        return this;
    }

    @NotNull
    @Override
    public DbMapCounterKey<K, E> register() {
        DbMapCounterKeyImpl<K, E> key = new DbMapCounterKeyImpl<>(this.id, this.type, this.entryCodec, this.scale);
        this.type.keys().register(key);
        return key;
    }
}