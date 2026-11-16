package net.momirealms.sparrow.data.internal.key;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.key.SortableMemoryDoubleKey;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public final class SortableMemoryDoubleKeyImpl<K> extends AbstractDataKey<K> implements SortableMemoryDoubleKey<K>, ValueKeyImpl<K, Double> {
    private final ValueForm.DoubleForm form;

    public SortableMemoryDoubleKeyImpl(@NotNull Key id, @NotNull EntityTypeImpl<K> type, @NotNull ValueForm.DoubleForm form) {
        super(id, type, true);
        this.form = form;
    }

    @NotNull
    @Override
    public ValueForm.DoubleForm form() {
        return this.form;
    }
}