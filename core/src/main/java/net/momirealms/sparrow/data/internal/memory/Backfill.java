package net.momirealms.sparrow.data.internal.memory;

import net.momirealms.sparrow.data.internal.EntityTypeImpl;
import net.momirealms.sparrow.data.internal.key.AbstractDataKey;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@ApiStatus.Internal
public final class Backfill<K> {
    private final EntityTypeImpl<K> type;

    // 运行中注册内存键. 先进键表, 之后 online 的实体按新的键集合加载; 再对已经持有的实体逐个补读这个键, 全部读完 future 才完成.
    // 补读期间收到的这个键的覆盖先缓冲, 补完槽再应用
    public Backfill(@NotNull EntityTypeImpl<K> type) {
        this.type = type;
    }

    @NotNull
    public CompletableFuture<Void> register(@NotNull AbstractDataKey<K> key) {
        OverrideService<K> overrides = this.type.overrides();
        Map<K, HeldEntity<K>> targets = overrides.beginBackfill(key);
        List<AbstractDataKey<K>> keys = List.of(key);
        CompletableFuture<?>[] reads = new CompletableFuture<?>[targets.size()];
        int index = 0;
        for (Map.Entry<K, HeldEntity<K>> target : targets.entrySet()) {
            K id = target.getKey();
            HeldEntity<K> held = target.getValue();
            reads[index++] = this.type.residence().loadSlots(held.ref(), keys).thenAccept(slots -> overrides.finishBackfill(id, held, key, slots));
        }
        return CompletableFuture.allOf(reads).whenComplete((ignored, error) -> overrides.endBackfill(key));
    }
}