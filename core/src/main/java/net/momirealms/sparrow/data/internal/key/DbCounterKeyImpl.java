package net.momirealms.sparrow.data.internal.key;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.key.DbCounterKey;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public final class DbCounterKeyImpl<K> extends AbstractDataKey<K> implements DbCounterKey<K>, ValueKeyImpl<K, Long> {

    public DbCounterKeyImpl(@NotNull Key id, @NotNull EntityTypeImpl<K> type) {
        super(id, type, false);
    }

    @NotNull
    @Override
    public ValueForm.LongForm form() {
        return ValueForm.LongForm.ZERO;
    }
}