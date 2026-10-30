package net.momirealms.sparrow.data.internal.memory;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

@ApiStatus.Internal
final class TaskChain {
    private final AtomicReference<CompletableFuture<?>> last = new AtomicReference<>(CompletableFuture.completedFuture(null));

    // 把异步任务排成一条链. 新任务等上一个任务结束才开始, 不管上一个成功还是失败, 它的异常已经交给了它自己的调用方.
    // 排队只是换掉 last 里的 future, 不占锁, 也不占线程
    @NotNull
    <T> CompletableFuture<T> then(@NotNull Supplier<CompletableFuture<T>> task) {
        CompletableFuture<Void> turn = new CompletableFuture<>();
        CompletableFuture<T> done = turn.thenCompose(ignored -> task.get());
        this.last.getAndSet(done).whenComplete((ignored, error) -> turn.complete(null));
        return done;
    }
}