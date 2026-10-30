package net.momirealms.sparrow.data.internal.key;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.key.MemoryKey;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public final class MemoryKeyImpl<K, T> extends AbstractDataKey<K> implements MemoryKey<K, T> {
    private final ValueForm<T> form;

    public MemoryKeyImpl(@NotNull Key id, @NotNull EntityTypeImpl<K> type, @NotNull ValueForm<T> form) {
        super(id, type, false);
        this.form = form;
    }

    @NotNull
    public ValueForm<T> form() {
        return this.form;
    }
}