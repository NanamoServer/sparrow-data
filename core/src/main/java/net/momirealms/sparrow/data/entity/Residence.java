package net.momirealms.sparrow.data.entity;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

/** 持有权的入口, 只由驻留方调用. 驻留方保证同一时刻只有一台服务器持有这个实体. */
public interface Residence<K> {

    @NotNull
    CompletableFuture<Void> online(@NotNull K id);

    @NotNull
    CompletableFuture<Void> offline(@NotNull K id);
}