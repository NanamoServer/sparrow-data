package net.momirealms.sparrow.data.internal.snapshot;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import net.momirealms.sparrow.redis.messagebroker.Logger;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

@ApiStatus.Internal
public final class SnapshotCache<K> {
    private static final Slot WHOLE = new Slot(null);

    private final Cache<Node<K>, Map<Slot, SingleFlightLoader<?>>> nodes;
    private final Logger logger;

    // 一个实体类型的快照. 一个实体的一个键是一个节点, 节点里有整个值(普通键的值, 或映射键的全部条目)和各个条目的快照,
    // 节点按最近一次 peek 计时过期. 失效和本服写入用 getIfPresentQuietly 查找, 不算访问, 没人看的快照照样会过期
    public SnapshotCache(@NotNull Duration expireAfterAccess, @NotNull Logger logger) {
        this.nodes = Caffeine.newBuilder().expireAfterAccess(expireAfterAccess).build();
        this.logger = logger;
    }

    // 普通键的值, 或映射键的全部条目
    @Nullable
    public <V> V peek(@NotNull K id, @NotNull String keyId, @NotNull Supplier<CompletableFuture<V>> loader) {
        return this.flight(id, keyId, WHOLE, loader).peek();
    }

    // 返回 null 表示还没加载或者条目不存在, 用 isEntryLoaded 区分
    @Nullable
    public <V> V peekEntry(@NotNull K id, @NotNull String keyId, @NotNull String entry, @NotNull Supplier<CompletableFuture<V>> loader) {
        return this.flight(id, keyId, new Slot(entry), loader).peek();
    }

    // 只查询, 不触发加载, 也不算访问
    public boolean isEntryLoaded(@NotNull K id, @NotNull String keyId, @NotNull String entry) {
        Map<Slot, SingleFlightLoader<?>> flights = this.nodes.policy().getIfPresentQuietly(new Node<>(id, keyId));
        if (flights == null) return false;
        SingleFlightLoader<?> flight = flights.get(new Slot(entry));
        return flight != null && flight.isLoaded();
    }

    // 本服写入后用返回值更新快照. 没人看过的对象不建快照; 整个映射的快照由随后的失效重读
    @SuppressWarnings("unchecked")
    public <V> void put(@NotNull K id, @NotNull String keyId, @Nullable String entry, @Nullable V value) {
        Map<Slot, SingleFlightLoader<?>> flights = this.nodes.policy().getIfPresentQuietly(new Node<>(id, keyId));
        if (flights == null) return;
        SingleFlightLoader<V> flight = (SingleFlightLoader<V>) flights.get(entry == null ? WHOLE : new Slot(entry));
        if (flight != null) {
            flight.put(value);
        }
    }

    // 范围和失效消息相同: entry 不为空是一个条目, 这时整个映射的快照也要重读; entry 为空是整个键;
    // keyId 为空是这个实体的全部键; id 为空是这个键在所有实体上. 快照里没有的对象没人在看, 不读
    public void invalidate(@Nullable K id, @Nullable String keyId, @Nullable String entry) {
        if (id != null && keyId != null) {
            Map<Slot, SingleFlightLoader<?>> flights = this.nodes.policy().getIfPresentQuietly(new Node<>(id, keyId));
            if (flights == null) return;
            if (entry == null) {
                invalidateAll(flights);
            } else {
                invalidateIfPresent(flights.get(new Slot(entry)));
                invalidateIfPresent(flights.get(WHOLE));
            }
            return;
        }
        // 删除整个实体或清除一个键时才会走到这里, 很少发生, 遍历全部节点
        this.nodes.asMap().forEach((node, flights) -> {
            if ((id == null || node.id().equals(id)) && (keyId == null || node.keyId().equals(keyId))) {
                invalidateAll(flights);
            }
        });
    }

    @SuppressWarnings("unchecked")
    @NotNull
    private <V> SingleFlightLoader<V> flight(@NotNull K id, @NotNull String keyId, @NotNull Slot slot, @NotNull Supplier<CompletableFuture<V>> loader) {
        Map<Slot, SingleFlightLoader<?>> flights = this.nodes.get(new Node<>(id, keyId), node -> new ConcurrentHashMap<>());
        return (SingleFlightLoader<V>) flights.computeIfAbsent(slot, created -> new SingleFlightLoader<>(loader, this.logger, describe(id, keyId, created)));
    }

    private static void invalidateIfPresent(@Nullable SingleFlightLoader<?> flight) {
        if (flight != null) {
            flight.invalidate();
        }
    }

    private static void invalidateAll(@NotNull Map<Slot, SingleFlightLoader<?>> flights) {
        for (SingleFlightLoader<?> flight : flights.values()) {
            flight.invalidate();
        }
    }

    @NotNull
    private static String describe(@NotNull Object id, @NotNull String keyId, @NotNull Slot slot) {
        return slot.entry() == null ? id + " " + keyId : id + " " + keyId + " [" + slot.entry() + "]";
    }

    private record Node<K>(@NotNull K id, @NotNull String keyId) {
    }

    // 节点里一个快照的位置. entry 为空表示整个值
    private record Slot(@Nullable String entry) {
    }
}