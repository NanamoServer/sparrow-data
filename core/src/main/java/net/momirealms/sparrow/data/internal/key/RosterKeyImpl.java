package net.momirealms.sparrow.data.internal.key;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.internal.roster.Roster;
import net.momirealms.sparrow.data.key.RosterKey;
import net.momirealms.sparrow.data.key.ValueCodec;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public final class RosterKeyImpl<K, V> extends AbstractDataKey<K> implements RosterKey<K, V> {
    private final ValueCodec<V> codec;
    private Roster<K, V> roster; // 注册时绑定, 键交给宿主之前已经设好

    public RosterKeyImpl(@NotNull Key id, @NotNull EntityTypeImpl<K> type, @NotNull ValueCodec<V> codec) {
        super(id, type, false);
        this.codec = codec;
    }

    @NotNull
    public ValueCodec<V> codec() {
        return this.codec;
    }

    @NotNull
    public Roster<K, V> roster() {
        return this.roster;
    }

    public void roster(@NotNull Roster<K, V> roster) {
        this.roster = roster;
    }

    @Override
    public boolean stored() {
        return false;
    }
}