package net.momirealms.sparrow.data.internal.key;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.key.SortableDbCounterKey;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public final class SortableDbCounterKeyImpl<K> extends AbstractDataKey<K> implements SortableDbCounterKey<K> {

    public SortableDbCounterKeyImpl(@NotNull Key id, @NotNull EntityTypeImpl<K> type) {
        super(id, type, true);
    }

    @NotNull
    public ValueForm.LongForm form() {
        return ValueForm.LongForm.ZERO;
    }
}