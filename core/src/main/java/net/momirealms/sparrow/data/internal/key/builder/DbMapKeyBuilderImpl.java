package net.momirealms.sparrow.data.internal.key.builder;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.internal.key.DbMapKeyImpl;
import net.momirealms.sparrow.data.internal.key.ValueForm;
import net.momirealms.sparrow.data.key.DbMapKey;
import net.momirealms.sparrow.data.key.EntryKeyCodec;
import net.momirealms.sparrow.data.key.builder.DbMapKeyBuilder;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public final class DbMapKeyBuilderImpl<K, E, V> implements DbMapKeyBuilder<K, E, V> {
    private final EntityTypeImpl<K> type;
    private final Key id;
    private final EntryKeyCodec<E> entryCodec;
    private final ValueForm<V> valueForm;

    public DbMapKeyBuilderImpl(@NotNull EntityTypeImpl<K> type, @NotNull Key id, @NotNull EntryKeyCodec<E> entryCodec, @NotNull ValueForm<V> valueForm) {
        this.type = type;
        this.id = id;
        this.entryCodec = entryCodec;
        this.valueForm = valueForm;
    }

    @NotNull
    @Override
    public DbMapKey<K, E, V> register() {
        DbMapKeyImpl<K, E, V> key = new DbMapKeyImpl<>(this.id, this.type, this.entryCodec, this.valueForm);
        this.type.keys().register(key);
        return key;
    }
}