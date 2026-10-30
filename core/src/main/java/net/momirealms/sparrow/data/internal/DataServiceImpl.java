package net.momirealms.sparrow.data.internal;

import io.lettuce.core.api.StatefulRedisConnection;
import io.netty.buffer.ByteBuf;
import net.momirealms.sparrow.data.DataService;
import net.momirealms.sparrow.data.Transaction;
import net.momirealms.sparrow.data.entity.EntityIdCodec;
import net.momirealms.sparrow.data.entity.EntityType;
import net.momirealms.sparrow.data.internal.db.TransactionImpl;
import net.momirealms.sparrow.data.internal.memory.AutoSaver;
import net.momirealms.sparrow.data.internal.redis.ReplicaStore;
import net.momirealms.sparrow.data.internal.sync.DataMessenger;
import net.momirealms.sparrow.data.storage.EntityStorage;
import net.momirealms.sparrow.redis.messagebroker.Logger;
import net.momirealms.sparrow.redis.messagebroker.MessageBroker;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Predicate;
import java.util.regex.Pattern;

@ApiStatus.Internal
public final class DataServiceImpl implements DataService {
    private static final Pattern ENTITY_TYPE_NAME = Pattern.compile("[a-z0-9_]{1,32}");

    private final EntityStorage storage;
    private final Executor executor;
    private final ReplicaStore replicas;
    private final DataMessenger<?> messenger;
    private final Logger logger;
    private final AutoSaver autoSaver;
    private final Duration snapshotExpiry;

    private final Map<String, EntityTypeImpl<?>> entityTypes = new ConcurrentHashMap<>();

    // 组装数据层. 构造时在 broker 上注册失效和覆盖消息, 所以同一个频道上的服务器要在同样的位置构造, 并且在 broker.subscribe() 之前
    public DataServiceImpl(
            @NotNull EntityStorage storage,
            @NotNull Executor executor,
            @NotNull StatefulRedisConnection<byte[], byte[]> redis,
            @NotNull MessageBroker<? extends ByteBuf> broker,
            @NotNull Logger logger,
            @NotNull Duration saveInterval,
            @NotNull Duration replicaTtl,
            @NotNull Duration snapshotExpiry
    ) {
        this.storage = storage;
        this.executor = executor;
        this.replicas = new ReplicaStore(redis, replicaTtl);
        this.messenger = DataMessenger.register(broker);
        this.logger = logger;
        this.autoSaver = new AutoSaver(saveInterval);
        this.snapshotExpiry = snapshotExpiry;
    }

    // 类型名会拼进 SQL 表名, 限制为 1 到 32 个 [a-z0-9_] 字符. 存储的准备在调用线程上同步执行, 完成后才开始接收这个类型的消息
    @NotNull
    @Override
    public synchronized <K> EntityType<K> registerEntityType(@NotNull String name, @NotNull EntityIdCodec<K> idCodec) {
        if (!ENTITY_TYPE_NAME.matcher(name).matches()) throw new IllegalArgumentException("Entity type name must be 1 to 32 characters of [a-z0-9_]: " + name);
        if (this.entityTypes.containsKey(name)) throw new IllegalArgumentException("Entity type is already registered: " + name);
        this.storage.prepare(name);
        EntityTypeImpl<K> type = new EntityTypeImpl<>(this, name, idCodec);
        this.entityTypes.put(name, type);
        this.messenger.listen(name, type);
        return type;
    }

    @NotNull
    @Override
    public CompletableFuture<Boolean> transaction(@NotNull Predicate<Transaction> body) {
        return CompletableFuture.supplyAsync(() -> new TransactionImpl(this.storage).run(body), this.executor);
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @NotNull
    public EntityStorage storage() {
        return this.storage;
    }

    @NotNull
    public Executor executor() {
        return this.executor;
    }

    @NotNull
    public ReplicaStore replicas() {
        return this.replicas;
    }

    @NotNull
    public DataMessenger<?> messenger() {
        return this.messenger;
    }

    @NotNull
    public Logger logger() {
        return this.logger;
    }

    @NotNull
    public AutoSaver autoSaver() {
        return this.autoSaver;
    }

    @NotNull
    public Duration snapshotExpiry() {
        return this.snapshotExpiry;
    }
}