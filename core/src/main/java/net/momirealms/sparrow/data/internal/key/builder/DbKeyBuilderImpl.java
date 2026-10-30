package net.momirealms.sparrow.data.internal.key.builder;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.internal.key.DbKeyImpl;
import net.momirealms.sparrow.data.internal.key.ValueForm;
import net.momirealms.sparrow.data.key.DbKey;
import net.momirealms.sparrow.data.key.builder.DbKeyBuilder;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public final class DbKeyBuilderImpl<K, T> implements DbKeyBuilder<K, T> {
    private final EntityTypeImpl<K> type;
    private final Key id;
    private final ValueForm<T> form;

    public DbKeyBuilderImpl(@NotNull EntityTypeImpl<K> type, @NotNull Key id, @NotNull ValueForm<T> form) {
        this.type = type;
        this.id = id;
        this.form = form;
    }

    @NotNull
    @Override
    public DbKey<K, T> register() {
        DbKeyImpl<K, T> key = new DbKeyImpl<>(this.id, this.type, this.form);
        this.type.keys().register(key);
        return key;
    }
}