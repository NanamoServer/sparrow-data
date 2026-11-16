package net.momirealms.sparrow.data;

import io.lettuce.core.RedisClient;
import net.momirealms.sparrow.data.internal.DataServiceImpl;
import net.momirealms.sparrow.data.storage.EntityStorage;
import net.momirealms.sparrow.message.MessageBroker;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Predicate;

public final class SparrowDataService {

    private SparrowDataService() {
    }

    // 数据层的入口. 宿主传入的存储, 线程和 broker 都由宿主创建, 也由宿主在 DataService.close() 之后关闭
    @NotNull
    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private static final Duration DEFAULT_SNAPSHOT_EXPIRY = Duration.ofMinutes(5);

        private EntityStorage storage;
        private Executor executor;
        private MessageBroker messageBroker;
        private Duration saveInterval;
        private Duration snapshotExpiry = DEFAULT_SNAPSHOT_EXPIRY;
        private @Nullable RedisClient redis;
        private @Nullable Predicate<String> serverAlive;

        private Builder() {
        }

        @NotNull
        public Builder storage(@NotNull EntityStorage storage) {
            this.storage = storage;
            return this;
        }

        // 调用存储接口的线程
        @NotNull
        public Builder executor(@NotNull Executor executor) {
            this.executor = executor;
            return this;
        }

        @NotNull
        public Builder messageBroker(@NotNull MessageBroker messageBroker) {
            this.messageBroker = messageBroker;
            return this;
        }

        // 每个持有中的实体从 online 开始, 每隔这么久落库一次
        @NotNull
        public Builder saveInterval(@NotNull Duration saveInterval) {
            this.saveInterval = saveInterval;
            return this;
        }

        // 快照在最近一次 peek 之后多久没人看就丢掉, 不设时为 5 分钟
        @NotNull
        public Builder snapshotExpiry(@NotNull Duration snapshotExpiry) {
            this.snapshotExpiry = snapshotExpiry;
            return this;
        }

        // 名单键用它读写 Redis, 不用名单键时可以不传. 数据层在上面自己开一条命令连接, close 时只关这条连接, client 仍由宿主关闭.
        // <strong>必须和 messageBroker 的连接来自同一个 client</strong>, 数据层靠它的连接事件发现订阅断开过
        @NotNull
        public Builder redis(@NotNull RedisClient redis) {
            this.redis = redis;
            return this;
        }

        // 判断一台服务器是否存活, 参数是服务器 ID. 名单里所属服务器不存活的记录在读取时被过滤掉, 不设时不过滤.
        // 每次读取名单都会调用, 可能在任意线程上, 要快, 并且线程安全
        @NotNull
        public Builder serverAlive(@NotNull Predicate<String> serverAlive) {
            this.serverAlive = serverAlive;
            return this;
        }

        // 会在 broker 上注册数据层的消息. 同一个频道上的服务器要在同样的位置调用, 并且在 broker.subscribe() 之前.
        // storage, executor, messageBroker 和 saveInterval 是必填的, 缺了时抛 IllegalStateException, 缺的一次全部列出来
        @NotNull
        public DataService build() {
            List<String> missing = new ArrayList<>();
            if (this.storage == null) missing.add("storage");
            if (this.executor == null) missing.add("executor");
            if (this.messageBroker == null) missing.add("messageBroker");
            if (this.saveInterval == null) missing.add("saveInterval");
            if (!missing.isEmpty()) throw new IllegalStateException("Missing required settings: " + String.join(", ", missing));
            return new DataServiceImpl(
                    this.storage,
                    this.executor,
                    this.messageBroker,
                    this.saveInterval,
                    this.snapshotExpiry,
                    this.redis,
                    this.serverAlive
            );
        }
    }
}