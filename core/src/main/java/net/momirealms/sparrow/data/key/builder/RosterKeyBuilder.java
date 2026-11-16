package net.momirealms.sparrow.data.key.builder;

import net.momirealms.sparrow.data.key.RosterKey;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

/** 注册 {@link RosterKey} 的构建器. register 会清掉本服上次运行留下的记录并装载镜像, future 在镜像可以查询时完成. */
public interface RosterKeyBuilder<K, V> {

    // <strong>要在 broker.subscribe() 之后调用</strong>, 镜像靠订阅到的消息跟上装载期间的变化
    @NotNull
    CompletableFuture<RosterKey<K, V>> register();
}