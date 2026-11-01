package net.momirealms.sparrow.data.entity;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

public interface Watch extends AutoCloseable {

    // 被观察的值可能变了时调用 onChange. 回调在触发改动的线程上同步执行, 可能是调用 write 的线程, Redis 的订阅线程或宿主的 Executor, 要快, 并且线程安全.
    // ready 在被观察的值第一次能用 peek 读到时完成, 本服持有或快照已经加载过时 watch 返回之前就已完成, 否则等首次加载的结果被接受.
    // 首次加载失败时带着存储抛出的异常失败; 完成之前 watch 被关闭, 键被注销或数据层关闭时以 CancellationException 结束.
    // 每次调用返回同一个 future. 它可能在完成加载的 Executor, Redis 的订阅线程或关闭它的线程上完成, 后续步骤要自己切到需要的线程
    @NotNull
    CompletableFuture<Void> ready();

    // 关闭之后不再通知, 重复关闭没有效果. 注销被观察的键时数据层会通知最后一次再自动关闭
    @Override
    void close();
}