package net.momirealms.sparrow.data.entity;

import net.momirealms.sparrow.data.key.DbCounterKey;
import net.momirealms.sparrow.data.key.DbKey;
import net.momirealms.sparrow.data.key.DbMapCounterKey;
import net.momirealms.sparrow.data.key.DbMapKey;
import net.momirealms.sparrow.data.key.MapKey;
import net.momirealms.sparrow.data.key.MemoryDataKey;
import net.momirealms.sparrow.data.key.MemoryKey;
import net.momirealms.sparrow.data.key.MemoryMapKey;
import net.momirealms.sparrow.data.key.RosterKey;
import net.momirealms.sparrow.data.key.ValueKey;
import net.momirealms.sparrow.data.key.ViewKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;

/** 实体句柄, 由类型和 ID 组成, 不带状态, 随时可以取. 能调用哪些方法由传入的键决定. */
public interface Entity<K> {

    @NotNull
    EntityType<K> type();

    @NotNull
    K id();

    boolean isHeld();

    // 读到的值由持有容器和快照共享, 不能原地修改, BYTES 读出的数组也一样. 要改就在 update 或 compute 里返回新值
    @NotNull
    <T> T get(@NotNull MemoryKey<K, T> key);

    <T> void write(@NotNull MemoryKey<K, T> key, @NotNull T value);

    @NotNull
    <T> T update(@NotNull MemoryKey<K, T> key, @NotNull UnaryOperator<T> operator);

    @Nullable
    <E, V> V get(@NotNull MemoryMapKey<K, E, V> key, @NotNull E entry);

    @NotNull
    <E, V> Map<E, V> getAll(@NotNull MemoryMapKey<K, E, V> key);

    <E, V> void put(@NotNull MemoryMapKey<K, E, V> key, @NotNull E entry, @NotNull V value);

    <E, V> void remove(@NotNull MemoryMapKey<K, E, V> key, @NotNull E entry);

    @Nullable
    <E, V> V compute(@NotNull MemoryMapKey<K, E, V> key, @NotNull E entry, @NotNull UnaryOperator<@Nullable V> operator);

    void publish(@NotNull MemoryDataKey<K> key);

    @NotNull
    CompletableFuture<Void> save();

    @NotNull
    <T> CompletableFuture<Void> override(@NotNull MemoryKey<K, T> key, @NotNull T value);

    @NotNull
    CompletableFuture<Void> reset(@NotNull MemoryKey<K, ?> key);

    @NotNull
    <E, V> CompletableFuture<Void> override(@NotNull MemoryMapKey<K, E, V> key, @NotNull E entry, @NotNull V value);

    @NotNull
    <E> CompletableFuture<Void> reset(@NotNull MemoryMapKey<K, E, ?> key, @NotNull E entry);

    @NotNull
    <T> Watch watch(@NotNull ValueKey<K, T> key, @NotNull Runnable onChange);

    @NotNull
    <E> Watch watch(@NotNull MapKey<K, E, ?> key, @NotNull E entry, @NotNull Runnable onChange);

    @NotNull
    <T> Pin pin(@NotNull ValueKey<K, T> key);

    @NotNull
    <E> Pin pin(@NotNull MapKey<K, E, ?> key, @NotNull E entry);

    // 视图背后的数据变了之后调用. 本服有这个快照时重读, 并通知其他服务器重读.
    // future 在本服这次重读写进快照后完成, 重读失败时带着 loader 的异常失败, 快照保留旧值; 本服没有这个快照时立即完成
    @NotNull
    CompletableFuture<Void> invalidate(@NotNull ViewKey<K, ?> key);

    // 把这个实体写进名单, 记录归本服所有, 已经在名单里时换成新值并接管归属. 本服的查询在返回之前就能看到,
    // future 在 Redis 完成写入和广播后完成, 不等其他服务器处理
    @NotNull
    <V> CompletableFuture<Void> announce(@NotNull RosterKey<K, V> key, @NotNull V value);

    // 把这个实体移出名单. 只撤回归本服所有的记录, 已经被别的服务器接管时什么都不做
    @NotNull
    CompletableFuture<Void> withdraw(@NotNull RosterKey<K, ?> key);

    // 同步读本服镜像, 不在名单里时返回 null. 所属服务器被判定为不存活的记录也当作不在
    @Nullable
    <V> V member(@NotNull RosterKey<K, V> key);

    // 这个实体进出名单或者值变了时调用 onChange, ready 总是已经完成
    @NotNull
    Watch watch(@NotNull RosterKey<K, ?> key, @NotNull Runnable onChange);

    @Nullable
    <T> T peek(@NotNull ValueKey<K, T> key);

    @Nullable
    <E, V> V peek(@NotNull MapKey<K, E, V> key, @NotNull E entry);

    <E> boolean isLoaded(@NotNull MapKey<K, E, ?> key, @NotNull E entry);

    @Nullable
    <E, V> Map<E, V> peekAll(@NotNull MapKey<K, E, V> key);

    @NotNull
    <T> CompletableFuture<T> read(@NotNull ValueKey<K, T> key);

    @NotNull
    <E, V> CompletableFuture<@Nullable V> read(@NotNull MapKey<K, E, V> key, @NotNull E entry);

    @NotNull
    <E, V> CompletableFuture<Map<E, V>> readAll(@NotNull MapKey<K, E, V> key);

    @NotNull
    <T> CompletableFuture<Void> set(@NotNull DbKey<K, T> key, @NotNull T value);

    @NotNull
    <T> CompletableFuture<T> update(@NotNull DbKey<K, T> key, @NotNull UnaryOperator<T> operator);

    @NotNull
    CompletableFuture<Void> set(@NotNull DbCounterKey<K> key, long value);

    @NotNull
    CompletableFuture<Long> add(@NotNull DbCounterKey<K> key, long delta);

    @NotNull
    CompletableFuture<OptionalLong> take(@NotNull DbCounterKey<K> key, long amount);

    @NotNull
    <E, V> CompletableFuture<Void> put(@NotNull DbMapKey<K, E, V> key, @NotNull E entry, @NotNull V value);

    @NotNull
    <E, V> CompletableFuture<Boolean> putIfAbsent(@NotNull DbMapKey<K, E, V> key, @NotNull E entry, @NotNull V value);

    @NotNull
    <E, V> CompletableFuture<@Nullable V> update(@NotNull DbMapKey<K, E, V> key, @NotNull E entry, @NotNull UnaryOperator<V> operator);

    @NotNull
    <E> CompletableFuture<Boolean> remove(@NotNull DbMapKey<K, E, ?> key, @NotNull E entry);

    @NotNull
    <E> CompletableFuture<Void> set(@NotNull DbMapCounterKey<K, E> key, @NotNull E entry, long value);

    @NotNull
    <E> CompletableFuture<Long> add(@NotNull DbMapCounterKey<K, E> key, @NotNull E entry, long delta);

    @NotNull
    <E> CompletableFuture<OptionalLong> take(@NotNull DbMapCounterKey<K, E> key, @NotNull E entry, long amount);

    @NotNull
    <E> CompletableFuture<Boolean> remove(@NotNull DbMapCounterKey<K, E> key, @NotNull E entry);

    @NotNull
    CompletableFuture<Void> delete();
}