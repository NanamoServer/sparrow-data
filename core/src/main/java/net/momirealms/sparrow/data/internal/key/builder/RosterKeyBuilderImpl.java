package net.momirealms.sparrow.data.internal.key.builder;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.internal.key.RosterKeyImpl;
import net.momirealms.sparrow.data.internal.roster.RosterService;
import net.momirealms.sparrow.data.key.RosterKey;
import net.momirealms.sparrow.data.key.ValueCodec;
import net.momirealms.sparrow.data.key.builder.RosterKeyBuilder;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

@ApiStatus.Internal
public final class RosterKeyBuilderImpl<K, V> implements RosterKeyBuilder<K, V> {
    private final EntityTypeImpl<K> type;
    private final Key id;
    private final ValueCodec<V> codec;

    public RosterKeyBuilderImpl(@NotNull EntityTypeImpl<K> type, @NotNull Key id, @NotNull ValueCodec<V> codec) {
        this.type = type;
        this.id = id;
        this.codec = codec;
    }

    // 先确认 builder 里传了 redis, 再登记进键表, 没传时键表不会多出一个用不了的键
    @NotNull
    @Override
    public CompletableFuture<RosterKey<K, V>> register() {
        RosterService rosters = this.type.service().rosters();
        RosterKeyImpl<K, V> key = new RosterKeyImpl<>(this.id, this.type, this.codec);
        this.type.keys().register(key);
        return rosters.open(key).thenApply(ignored -> key);
    }
}