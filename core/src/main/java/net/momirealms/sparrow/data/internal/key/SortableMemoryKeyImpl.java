package net.momirealms.sparrow.data.internal.key;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.key.SortableMemoryKey;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public final class SortableMemoryKeyImpl<K> extends AbstractDataKey<K> implements SortableMemoryKey<K>, ValueKeyImpl<K, Long> {
    private final ValueForm.LongForm form;

    public SortableMemoryKeyImpl(@NotNull Key id, @NotNull EntityTypeImpl<K> type, @NotNull ValueForm.LongForm form) {
        super(id, type, true);
        this.form = form;
    }

    @NotNull
    @Override
    public ValueForm.LongForm form() {
        return this.form;
    }
}