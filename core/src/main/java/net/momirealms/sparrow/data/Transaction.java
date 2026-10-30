package net.momirealms.sparrow.data;

import net.momirealms.sparrow.data.entity.Entity;
import net.momirealms.sparrow.data.key.DbCounterKey;
import net.momirealms.sparrow.data.key.DbKey;
import net.momirealms.sparrow.data.key.DbMapCounterKey;
import net.momirealms.sparrow.data.key.DbMapKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.OptionalLong;
import java.util.function.UnaryOperator;

/** 事务内的 DB 权威操作. 方法和 {@link Entity} 上的同名方法一一对应, 区别是同步执行, 第一个参数是实体. */
public interface Transaction {

    <K, T> void set(@NotNull Entity<K> entity, @NotNull DbKey<K, T> key, @NotNull T value);

    @NotNull
    <K, T> T update(@NotNull Entity<K> entity, @NotNull DbKey<K, T> key, @NotNull UnaryOperator<T> operator);

    <K> void set(@NotNull Entity<K> entity, @NotNull DbCounterKey<K> key, long value);

    <K> long add(@NotNull Entity<K> entity, @NotNull DbCounterKey<K> key, long delta);

    @NotNull
    <K> OptionalLong take(@NotNull Entity<K> entity, @NotNull DbCounterKey<K> key, long amount);

    <K, E, V> void put(@NotNull Entity<K> entity, @NotNull DbMapKey<K, E, V> key, @NotNull E entry, @NotNull V value);

    <K, E, V> boolean putIfAbsent(@NotNull Entity<K> entity, @NotNull DbMapKey<K, E, V> key, @NotNull E entry, @NotNull V value);

    @Nullable
    <K, E, V> V update(@NotNull Entity<K> entity, @NotNull DbMapKey<K, E, V> key, @NotNull E entry, @NotNull UnaryOperator<V> operator);

    <K, E> boolean remove(@NotNull Entity<K> entity, @NotNull DbMapKey<K, E, ?> key, @NotNull E entry);

    <K, E> void set(@NotNull Entity<K> entity, @NotNull DbMapCounterKey<K, E> key, @NotNull E entry, long value);

    <K, E> long add(@NotNull Entity<K> entity, @NotNull DbMapCounterKey<K, E> key, @NotNull E entry, long delta);

    @NotNull
    <K, E> OptionalLong take(@NotNull Entity<K> entity, @NotNull DbMapCounterKey<K, E> key, @NotNull E entry, long amount);

    <K, E> boolean remove(@NotNull Entity<K> entity, @NotNull DbMapCounterKey<K, E> key, @NotNull E entry);
}