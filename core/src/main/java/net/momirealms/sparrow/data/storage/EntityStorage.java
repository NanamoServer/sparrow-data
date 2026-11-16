package net.momirealms.sparrow.data.storage;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.function.Predicate;

/** 持久化服务的接口, 由内置的 SQL 实现提供, <strong>不供宿主自行实现</strong>. 数据层在宿主提供的线程上同步调用. */
@ApiStatus.NonExtendable
public interface EntityStorage {

    void prepare(@NotNull String entityType);

    @NotNull
    StoredEntity load(@NotNull EntityRef entity, @NotNull Collection<StorageKey> keys);

    void save(@NotNull EntityRef entity, @NotNull SaveBatch batch);

    @Nullable
    StoredValue read(@NotNull EntityRef entity, @NotNull StorageKey key, @Nullable String entry);

    @NotNull
    Map<String, StoredValue> readEntries(@NotNull EntityRef entity, @NotNull StorageKey key);

    void set(@NotNull EntityRef entity, @NotNull StorageKey key, @Nullable String entry, @NotNull StoredValue value);

    boolean compareAndSet(@NotNull EntityRef entity, @NotNull StorageKey key, @Nullable String entry, byte @Nullable [] expected, byte @NotNull [] value);

    boolean compareAndRemove(@NotNull EntityRef entity, @NotNull StorageKey key, @Nullable String entry, byte @NotNull [] expected);

    boolean remove(@NotNull EntityRef entity, @NotNull StorageKey key, @Nullable String entry);

    long add(@NotNull EntityRef entity, @NotNull StorageKey key, @Nullable String entry, long delta);

    @NotNull
    OptionalLong take(@NotNull EntityRef entity, @NotNull StorageKey key, @Nullable String entry, long amount);

    @NotNull
    List<RankRow> top(@NotNull String entityType, @NotNull StorageKey key, int offset, int limit);

    long countAbove(@NotNull String entityType, @NotNull StorageKey key, long value);

    void deleteEntity(@NotNull EntityRef entity);

    // 跨所有实体删除这个键 ID 下的全部数据, 普通值, 映射条目和排行数据都删. 调用时键已经注销, 数据层不知道它是不是 sortable
    void purge(@NotNull String entityType, @NotNull String keyId);

    boolean transaction(@NotNull Predicate<EntityStorage> body);
}