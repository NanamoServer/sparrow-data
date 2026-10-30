package net.momirealms.sparrow.data.internal.key;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.key.DbMapCounterKey;
import net.momirealms.sparrow.data.key.EntryKeyCodec;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public final class DbMapCounterKeyImpl<K, E> extends AbstractDataKey<K> implements DbMapCounterKey<K, E> {
    private final EntryKeyCodec<E> entryCodec;

    public DbMapCounterKeyImpl(@NotNull Key id, @NotNull EntityTypeImpl<K> type, @NotNull EntryKeyCodec<E> entryCodec) {
        super(id, type, false);
        this.entryCodec = entryCodec;
    }

    @NotNull
    public EntryKeyCodec<E> entryCodec() {
        return this.entryCodec;
    }

    @NotNull
    public ValueForm.LongForm valueForm() {
        return ValueForm.LongForm.ZERO;
    }
}