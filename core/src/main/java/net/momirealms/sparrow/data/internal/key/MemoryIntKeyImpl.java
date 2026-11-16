package net.momirealms.sparrow.data.internal.key;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.key.MemoryIntKey;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public final class MemoryIntKeyImpl<K> extends AbstractDataKey<K> implements MemoryIntKey<K>, ValueKeyImpl<K, Integer> {
    private final ValueForm.IntForm form;

    public MemoryIntKeyImpl(@NotNull Key id, @NotNull EntityTypeImpl<K> type, @NotNull ValueForm.IntForm form) {
        super(id, type, false);
        this.form = form;
    }

    @NotNull
    @Override
    public ValueForm.IntForm form() {
        return this.form;
    }
}