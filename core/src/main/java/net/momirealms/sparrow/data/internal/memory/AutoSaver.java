package net.momirealms.sparrow.data.internal.memory;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@ApiStatus.Internal
public final class AutoSaver {
    private final ScheduledThreadPoolExecutor scheduler;
    private final long intervalMillis;
    private final Map<HeldEntity<?>, ScheduledFuture<?>> tasks = new ConcurrentHashMap<>();

    // 每个持有中的实体一个重复任务, online 之后每隔一个保存间隔落库一次. 实体 online 的时间各不相同, 落库自然分散在整个间隔里.
    // 任务在数据层自己的调度线程上运行, 只负责把落库排进实体的落库链, 存储调用在宿主的 Executor 上进行
    public AutoSaver(@NotNull Duration interval) {
        this.scheduler = new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread thread = new Thread(runnable, "sparrow-data-auto-save");
            thread.setDaemon(true);
            return thread;
        });
        // offline 时取消的任务立刻移出队列
        this.scheduler.setRemoveOnCancelPolicy(true);
        this.intervalMillis = interval.toMillis();
    }

    public void start(@NotNull HeldEntity<?> held, @NotNull Runnable save) {
        this.tasks.put(held, this.scheduler.scheduleWithFixedDelay(save, this.intervalMillis, this.intervalMillis, TimeUnit.MILLISECONDS));
    }

    public void stop(@NotNull HeldEntity<?> held) {
        this.tasks.remove(held).cancel(false);
    }
}