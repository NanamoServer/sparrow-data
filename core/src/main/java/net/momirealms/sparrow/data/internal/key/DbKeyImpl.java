package net.momirealms.sparrow.data.internal.key;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.key.DbKey;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public final class DbKeyImpl<K, T> extends AbstractDataKey<K> implements DbKey<K, T>, ValueKeyImpl<K, T> {
    private final ValueForm<T> form;

    public DbKeyImpl(@NotNull Key id, @NotNull EntityTypeImpl<K> type, @NotNull ValueForm<T> form) {
        super(id, type, false);
        this.form = form;
    }

    @NotNull
    @Override
    public ValueForm<T> form() {
        return this.form;
    }
}