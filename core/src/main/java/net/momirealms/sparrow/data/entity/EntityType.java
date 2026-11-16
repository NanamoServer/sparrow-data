package net.momirealms.sparrow.data.entity;

import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.RankEntry;
import net.momirealms.sparrow.data.key.DataKey;
import net.momirealms.sparrow.data.key.EntryKeyCodec;
import net.momirealms.sparrow.data.key.RosterKey;
import net.momirealms.sparrow.data.key.SortableKey;
import net.momirealms.sparrow.data.key.ValueCodec;
import net.momirealms.sparrow.data.key.builder.DbCounterKeyBuilder;
import net.momirealms.sparrow.data.key.builder.DbKeyBuilder;
import net.momirealms.sparrow.data.key.builder.DbMapCounterKeyBuilder;
import net.momirealms.sparrow.data.key.builder.DbMapKeyBuilder;
import net.momirealms.sparrow.data.key.builder.MemoryKeyBuilder;
import net.momirealms.sparrow.data.key.builder.MemoryLongKeyBuilder;
import net.momirealms.sparrow.data.key.builder.MemoryMapKeyBuilder;
import net.momirealms.sparrow.data.key.builder.RosterKeyBuilder;
import net.momirealms.sparrow.data.key.builder.ViewKeyBuilder;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public interface EntityType<K> {

    @NotNull
    String name();

    @NotNull
    Entity<K> entity(@NotNull K id);

    @NotNull
    Residence<K> residence();

    @NotNull
    <T> MemoryKeyBuilder<K, T> memory(@NotNull Key id, @NotNull ValueCodec<T> codec, @NotNull T defaultValue);

    @NotNull
    <T> MemoryKeyBuilder<K, Optional<T>> memory(@NotNull Key id, @NotNull ValueCodec<T> codec);

    @NotNull
    MemoryLongKeyBuilder<K> memoryLong(@NotNull Key id);

    @NotNull
    <E, V> MemoryMapKeyBuilder<K, E, V> memoryMap(@NotNull Key id, @NotNull EntryKeyCodec<E> entryCodec, @NotNull ValueCodec<V> valueCodec);

    @NotNull
    <T> DbKeyBuilder<K, T> db(@NotNull Key id, @NotNull ValueCodec<T> codec, @NotNull T defaultValue);

    @NotNull
    <T> DbKeyBuilder<K, Optional<T>> db(@NotNull Key id, @NotNull ValueCodec<T> codec);

    @NotNull
    DbCounterKeyBuilder<K> dbCounter(@NotNull Key id);

    @NotNull
    <E, V> DbMapKeyBuilder<K, E, V> dbMap(@NotNull Key id, @NotNull EntryKeyCodec<E> entryCodec, @NotNull ValueCodec<V> valueCodec);

    @NotNull
    <E> DbMapCounterKeyBuilder<K, E> dbMapCounter(@NotNull Key id, @NotNull EntryKeyCodec<E> entryCodec);

    // loader 按实体 ID 从宿主自己的存储里读出当前数据, 结果不能是 null, 没有数据时返回空集合或 Optional.empty().
    // 冷加载, 收到失效和每次 read 都会调用它, 可能在任意线程上, 它自己负责离开调用线程.
    // 读出的值由所有读取方共享, 只能当作不可变对象使用, 并且要有按内容比较的 equals, watch 靠它判断值有没有变
    @NotNull
    <T> ViewKeyBuilder<K, T> view(@NotNull Key id, @NotNull Function<K, CompletableFuture<T>> loader);

    // 名单键要求 builder 里传了 redis, 没传时抛 IllegalStateException. 值由所有读取方共享, 只能当作不可变对象使用
    @NotNull
    <V> RosterKeyBuilder<K, V> roster(@NotNull Key id, @NotNull ValueCodec<V> codec);

    // 这一刻名单里的全部成员, 读的是本服镜像. 返回的 Map 是一份不可修改的副本
    @NotNull
    <V> Map<K, V> members(@NotNull RosterKey<K, V> key);

    // 名单里任何成员进出或者值变了时调用 onChange. 回调的线程和要求与实体上的 watch 相同, ready 总是已经完成
    @NotNull
    Watch watch(@NotNull RosterKey<K, ?> key, @NotNull Runnable onChange);

    @NotNull
    CompletableFuture<Void> unregister(@NotNull DataKey<K> key);

    @NotNull
    CompletableFuture<Void> purge(@NotNull Key id);

    @NotNull
    CompletableFuture<List<RankEntry<K>>> top(@NotNull SortableKey<K> key, int offset, int limit);

    @NotNull
    CompletableFuture<Long> rankOf(@NotNull SortableKey<K> key, @NotNull K id);
}