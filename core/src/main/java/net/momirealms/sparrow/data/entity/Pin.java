package net.momirealms.sparrow.data.entity;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

/** 让一个值的快照保持加载, 关闭之前不过期. 只要同步读取, 不关心变化时用它, 要收到变化通知用 {@link Watch}. */
public interface Pin extends AutoCloseable {

    // ready 在这个值第一次能用 readCachedOrNull 读到时完成, 本服持有或快照已经加载过时返回之前就已完成, 否则等首次加载的结果被接受.
    // 首次加载失败时带着加载抛出的异常失败; 完成之前被关闭, 键被注销或数据层关闭时以 CancellationException 结束.
    // 每次调用返回同一个 future. 它可能在完成加载的 Executor, Redis 的订阅线程或关闭它的线程上完成, 后续步骤要自己切到需要的线程
    @NotNull
    CompletableFuture<Void> ready();

    // 重复关闭没有效果. 注销对应的键时数据层会自动关闭
    @Override
    void close();
}