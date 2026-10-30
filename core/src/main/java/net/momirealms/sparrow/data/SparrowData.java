package net.momirealms.sparrow.data;

import io.lettuce.core.api.StatefulRedisConnection;
import io.netty.buffer.ByteBuf;
import net.momirealms.sparrow.data.internal.DataServiceImpl;
import net.momirealms.sparrow.data.storage.EntityStorage;
import net.momirealms.sparrow.redis.messagebroker.Logger;
import net.momirealms.sparrow.redis.messagebroker.MessageBroker;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.concurrent.Executor;

public final class SparrowData {

    private SparrowData() {
    }

    // 数据层的入口. 宿主传入的存储, 线程, Redis 连接, broker 和 logger 都由宿主创建, 也由宿主在 DataService.close() 之后关闭
    @NotNull
    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private static final Duration DEFAULT_SNAPSHOT_EXPIRY = Duration.ofMinutes(5);

        private EntityStorage storage;
        private Executor executor;
        private StatefulRedisConnection<byte[], byte[]> redis;
        private MessageBroker<? extends ByteBuf> messageBroker;
        private Logger logger;
        private Duration saveInterval;
        private Duration replicaTtl;
        private Duration snapshotExpiry = DEFAULT_SNAPSHOT_EXPIRY;

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

        // 读写 Redis 副本的命令连接
        @NotNull
        public Builder redis(@NotNull StatefulRedisConnection<byte[], byte[]> redis) {
            this.redis = redis;
            return this;
        }

        @NotNull
        public Builder messageBroker(@NotNull MessageBroker<? extends ByteBuf> messageBroker) {
            this.messageBroker = messageBroker;
            return this;
        }

        @NotNull
        public Builder logger(@NotNull Logger logger) {
            this.logger = logger;
            return this;
        }

        // 每个持有中的实体从 online 开始, 每隔这么久落库一次
        @NotNull
        public Builder saveInterval(@NotNull Duration saveInterval) {
            this.saveInterval = saveInterval;
            return this;
        }

        // Redis 副本的 TTL, 必须大于 saveInterval
        @NotNull
        public Builder replicaTtl(@NotNull Duration replicaTtl) {
            this.replicaTtl = replicaTtl;
            return this;
        }

        // 快照在最近一次 peek 之后多久没人看就丢掉, 不设时为 5 分钟
        @NotNull
        public Builder snapshotExpiry(@NotNull Duration snapshotExpiry) {
            this.snapshotExpiry = snapshotExpiry;
            return this;
        }

        // 会在 broker 上注册数据层的消息. 同一个频道上的服务器要在同样的位置调用, 并且在 broker.subscribe() 之前
        @NotNull
        public DataService build() {
            return new DataServiceImpl(
                    this.storage,
                    this.executor,
                    this.redis,
                    this.messageBroker,
                    this.logger,
                    this.saveInterval,
                    this.replicaTtl,
                    this.snapshotExpiry
            );
        }
    }
}