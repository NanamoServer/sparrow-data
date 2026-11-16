package net.momirealms.sparrow.data.entity;

import net.momirealms.sparrow.data.key.DbCounterKey;
import net.momirealms.sparrow.data.key.DbKey;
import net.momirealms.sparrow.data.key.MemoryDataKey;
import net.momirealms.sparrow.data.key.MemoryDoubleKey;
import net.momirealms.sparrow.data.key.MemoryIntKey;
import net.momirealms.sparrow.data.key.MemoryKey;
import net.momirealms.sparrow.data.key.MemoryLongKey;
import net.momirealms.sparrow.data.key.RosterKey;
import net.momirealms.sparrow.data.key.ValueKey;
import net.momirealms.sparrow.data.key.ViewKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.DoubleUnaryOperator;
import java.util.function.IntUnaryOperator;
import java.util.function.LongUnaryOperator;
import java.util.function.UnaryOperator;

/** 实体句柄, 由类型和 ID 组成, 不带状态, 随时可以取. 能调用哪些方法由传入的键决定. */
public interface Entity<K> {

    @NotNull
    EntityType<K> type();

    @NotNull
    K id();

    boolean isHeld();

    // 读到的值由持有容器和快照共享, 不能原地修改, BYTES 读出的数组也一样. 要改就在 update 里返回新值.
    // 值能不能是 null 由键的类型决定: 注册时给了默认值的键永远读不到 null, 没给的键没有存储值时读到 null
    <T> T get(@NotNull MemoryKey<K, T> key);

    // 写入 null 是删除存储的值, 之后有默认值的键读到默认值, 没有的读到 null. override 和 DB 键的 set 同理
    <T> void write(@NotNull MemoryKey<K, T> key, T value);

    // operator 返回 null 同样是删除存储的值
    <T> T update(@NotNull MemoryKey<K, T> key, @NotNull UnaryOperator<T> operator);

    // int, long, double 三种内存键的读写不装箱. get 和 write 是下面这几个重载, 传这三种键时编译器会自动选中它们;
    // 读改写不能重载, 用 add 或者带类型后缀的 update. 只能在持有这个实体的服上调用
    int get(@NotNull MemoryIntKey<K> key);

    void write(@NotNull MemoryIntKey<K> key, int value);

    // 原子加减, 返回新值
    int add(@NotNull MemoryIntKey<K> key, int delta);

    int updateInt(@NotNull MemoryIntKey<K> key, @NotNull IntUnaryOperator operator);

    long get(@NotNull MemoryLongKey<K> key);

    void write(@NotNull MemoryLongKey<K> key, long value);

    // 原子加减, 返回新值
    long add(@NotNull MemoryLongKey<K> key, long delta);

    long updateLong(@NotNull MemoryLongKey<K> key, @NotNull LongUnaryOperator operator);

    double get(@NotNull MemoryDoubleKey<K> key);

    void write(@NotNull MemoryDoubleKey<K> key, double value);

    // 原子加减, 返回新值
    double add(@NotNull MemoryDoubleKey<K> key, double delta);

    double updateDouble(@NotNull MemoryDoubleKey<K> key, @NotNull DoubleUnaryOperator operator);

    void publish(@NotNull MemoryDataKey<K> key);

    @NotNull
    CompletableFuture<Void> save();

    @NotNull
    <T> CompletableFuture<Void> override(@NotNull MemoryKey<K, T> key, T value);

    @NotNull
    <T> Watch watch(@NotNull ValueKey<K, T> key, @NotNull Runnable onChange);

    @NotNull
    <T> Pin pin(@NotNull ValueKey<K, T> key);

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

    // 同步读本服缓存的值, 内存键在本服持有时读的是持有的值. 还没缓存时返回 null 并在后台开始加载.
    // 没有默认值的键缓存到的值本身也可能是 null, 要区分时用 isCached
    @Nullable
    <T> T readCachedOrNull(@NotNull ValueKey<K, T> key);

    // load 为 false 时只读已有的缓存, 没有也不发起加载, 也不算一次访问, 不会推迟缓存的过期
    @Nullable
    <T> T readCachedOrNull(@NotNull ValueKey<K, T> key, boolean load);

    // readCachedOrNull 现在能不能读到值. 只查询, 不触发加载
    boolean isCached(@NotNull ValueKey<K, ?> key);

    @NotNull
    <T> CompletableFuture<T> read(@NotNull ValueKey<K, T> key);

    @NotNull
    <T> CompletableFuture<Void> set(@NotNull DbKey<K, T> key, T value);

    @NotNull
    <T> CompletableFuture<T> update(@NotNull DbKey<K, T> key, @NotNull UnaryOperator<T> operator);

    @NotNull
    CompletableFuture<Void> set(@NotNull DbCounterKey<K> key, long value);

    @NotNull
    CompletableFuture<Long> add(@NotNull DbCounterKey<K> key, long delta);

    @NotNull
    CompletableFuture<OptionalLong> take(@NotNull DbCounterKey<K> key, long amount);

    @NotNull
    CompletableFuture<Void> delete();
}