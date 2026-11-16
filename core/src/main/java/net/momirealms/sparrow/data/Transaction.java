package net.momirealms.sparrow.data;

import net.momirealms.sparrow.data.entity.Entity;
import net.momirealms.sparrow.data.key.DbCounterKey;
import net.momirealms.sparrow.data.key.DbKey;
import org.jetbrains.annotations.NotNull;

import java.util.OptionalLong;
import java.util.function.UnaryOperator;

/** 事务内的 DB 权威操作. 方法和 {@link Entity} 上的同名方法一一对应, 区别是同步执行, 第一个参数是实体. */
public interface Transaction {

    <K, T> void set(@NotNull Entity<K> entity, @NotNull DbKey<K, T> key, T value);

    <K, T> T update(@NotNull Entity<K> entity, @NotNull DbKey<K, T> key, @NotNull UnaryOperator<T> operator);

    <K> void set(@NotNull Entity<K> entity, @NotNull DbCounterKey<K> key, long value);

    <K> long add(@NotNull Entity<K> entity, @NotNull DbCounterKey<K> key, long delta);

    @NotNull
    <K> OptionalLong take(@NotNull Entity<K> entity, @NotNull DbCounterKey<K> key, long amount);

    // 标记这次事务回滚. body 照常 return, 返回值仍然交给调用方, 已经执行的写入全部撤销
    void rollback();
}