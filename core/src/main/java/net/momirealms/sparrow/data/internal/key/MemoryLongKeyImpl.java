package net.momirealms.sparrow.data.internal.key;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.key.MemoryLongKey;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public final class MemoryLongKeyImpl<K> extends AbstractDataKey<K> implements MemoryLongKey<K>, ValueKeyImpl<K, Long> {
    private final ValueForm.LongForm form;
    private final int scale;

    public MemoryLongKeyImpl(@NotNull Key id, @NotNull EntityTypeImpl<K> type, @NotNull ValueForm.LongForm form, int scale) {
        super(id, type, false);
        this.form = form;
        this.scale = scale;
    }

    @NotNull
    @Override
    public ValueForm.LongForm form() {
        return this.form;
    }

    @Override
    public int scale() {
        return this.scale;
    }
}