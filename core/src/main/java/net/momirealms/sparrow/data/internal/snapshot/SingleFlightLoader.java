package net.momirealms.sparrow.data.internal.snapshot;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

@ApiStatus.Internal
public final class SingleFlightLoader<V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleFlightLoader.class);

    private final Supplier<CompletableFuture<V>> loader;
    private final String name;
    private final Runnable replaced;
    private final Consumer<Throwable> failed;

    private @Nullable V value;
    private boolean loaded;
    private boolean loading;
    private boolean reloadAfter;
    private boolean discardResult;
    private @Nullable CompletableFuture<Void> landing;      // 等在途这一轮读取的 reload 调用方
    private @Nullable CompletableFuture<Void> landingAfter; // 等下一轮读取的 reload 调用方

    // 一个快照对象的值和它的读取状态.
    // loader 不抛异常, 失败通过返回的 future 表达; 它只发起读取, 不在调用线程上等结果. 值被换掉之后在锁外调用 replaced,
    // 读取失败时在锁外调用 failed
    SingleFlightLoader(@NotNull Supplier<CompletableFuture<V>> loader, @NotNull String name, @NotNull Runnable replaced, @NotNull Consumer<Throwable> failed) {
        this.loader = loader;
        this.name = name;
        this.replaced = replaced;
        this.failed = failed;
    }

    // 返回当前的快照. 还没加载过时返回 null, 并发起第一次读取
    @Nullable
    public V peek() {
        V current;
        boolean start;
        synchronized (this) {
            current = this.value;
            start = !this.loaded && !this.loading;
            if (start) {
                this.beginLoad();
            }
        }
        if (start) {
            this.load();
        }
        return current;
    }

    public synchronized boolean isLoaded() {
        return this.loaded;
    }

    // 只查询, 不发起读取. 还没加载过时返回 null, 加载过时值本身可以是 null
    @Nullable
    public synchronized Loaded<V> loaded() {
        return this.loaded ? new Loaded<>(this.value) : null;
    }

    // 空闲时发起一次读取; 正在读时记下, 读完再读一次, 因为正在进行的读可能发生在这次修改之前
    public void invalidate() {
        boolean start;
        synchronized (this) {
            start = !this.loading;
            if (start) {
                this.beginLoad();
            } else {
                this.reloadAfter = true;
            }
        }
        if (start) {
            this.load();
        }
    }

    // 和 invalidate 一样发起或排上一轮读取, 另外返回一个 future, 在这次调用之后才发起的那一轮读取结束时完成.
    // 正在读的那一轮可能发生在修改之前, 所以这时等的是下一轮
    @NotNull
    public CompletableFuture<Void> reload() {
        CompletableFuture<Void> landed;
        boolean start;
        synchronized (this) {
            start = !this.loading;
            if (start) {
                this.beginLoad();
                this.landing = new CompletableFuture<>();
                landed = this.landing;
            } else {
                this.reloadAfter = true;
                if (this.landingAfter == null) {
                    this.landingAfter = new CompletableFuture<>();
                }
                landed = this.landingAfter;
            }
        }
        if (start) {
            this.load();
        }
        return landed;
    }

    // 本服写入后用返回值更新快照. 正在进行的读比这个值旧, 读完时丢弃它的结果
    public void put(@Nullable V value) {
        synchronized (this) {
            this.value = value;
            this.loaded = true;
            if (this.loading) {
                this.discardResult = true;
            }
        }
        this.replaced.run();
    }

    // 调用方持有锁
    private void beginLoad() {
        this.loading = true;
        this.reloadAfter = false;
        this.discardResult = false;
    }

    // 在锁外调用 loader. future 已经完成时 whenComplete 会在当前线程直接回调 complete
    private void load() {
        this.loader.get().whenComplete(this::complete);
    }

    // 先写入这次的结果, 再决定要不要再读. 这次的结果比快照里的新, 写进去不会倒退, 持续被修改时快照也能一路更新.
    // 读失败时不再重读, 等下一次失效或 peek, 等下一轮的 reload 调用方也跟着这次失败结束
    private void complete(@Nullable V result, @Nullable Throwable error) {
        boolean again;
        boolean accepted;
        CompletableFuture<Void> landed;
        CompletableFuture<Void> skipped = null;
        synchronized (this) {
            this.loading = false;
            accepted = error == null && !this.discardResult;
            if (accepted) {
                this.value = result;
                this.loaded = true;
            }
            again = (error == null) && this.reloadAfter;
            landed = this.landing;
            if (again) {
                this.beginLoad();
                this.landing = this.landingAfter;
            } else {
                this.landing = null;
                skipped = this.landingAfter;
            }
            this.landingAfter = null;
        }
        if (accepted) {
            this.replaced.run();
        }
        if (error != null) {
            LOGGER.error("Failed to load snapshot {}", this.name, error);
            this.failed.accept(error);
            if (landed != null) landed.completeExceptionally(error);
            if (skipped != null) skipped.completeExceptionally(error);
        } else if (landed != null) {
            landed.complete(null);
        }
        if (again) {
            this.load();
        }
    }

    // 加载过的快照值, 值本身可以是 null
    public record Loaded<V>(@Nullable V value) {
    }
}