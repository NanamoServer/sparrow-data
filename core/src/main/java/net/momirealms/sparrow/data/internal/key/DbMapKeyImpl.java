package net.momirealms.sparrow.data.internal.key;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.key.DbMapKey;
import net.momirealms.sparrow.data.key.EntryKeyCodec;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public final class DbMapKeyImpl<K, E, V> extends AbstractDataKey<K> implements DbMapKey<K, E, V> {
    private final EntryKeyCodec<E> entryCodec;
    private final ValueForm<V> valueForm;

    public DbMapKeyImpl(@NotNull Key id, @NotNull EntityTypeImpl<K> type, @NotNull EntryKeyCodec<E> entryCodec, @NotNull ValueForm<V> valueForm) {
        super(id, type, false);
        this.entryCodec = entryCodec;
        this.valueForm = valueForm;
    }

    @NotNull
    public EntryKeyCodec<E> entryCodec() {
        return this.entryCodec;
    }

    @NotNull
    public ValueForm<V> valueForm() {
        return this.valueForm;
    }
}