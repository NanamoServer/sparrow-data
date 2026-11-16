package net.momirealms.sparrow.data.internal;

import io.lettuce.core.RedisClient;
import net.momirealms.sparrow.data.DataService;
import net.momirealms.sparrow.data.Transaction;
import net.momirealms.sparrow.data.entity.EntityIdCodec;
import net.momirealms.sparrow.data.entity.EntityType;
import net.momirealms.sparrow.data.internal.db.TransactionImpl;
import net.momirealms.sparrow.data.internal.roster.RosterService;
import net.momirealms.sparrow.data.internal.sync.DataMessenger;
import net.momirealms.sparrow.data.storage.EntityStorage;
import net.momirealms.sparrow.message.MessageBroker;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

@ApiStatus.Internal
public final class DataServiceImpl implements DataService {
    private static final Pattern ENTITY_TYPE_NAME = Pattern.compile("[a-z0-9_]{1,32}");

    private final EntityStorage storage;
    private final Executor executor;
    private final DataMessenger messenger;
    private final Duration saveInterval;
    private final Duration snapshotExpiry;
    private final @Nullable RosterService rosters; // 宿主没有传 redis 时为 null, 这时不能注册名单键

    private final Map<String, EntityTypeImpl<?>> entityTypes = new ConcurrentHashMap<>();

    // 组装数据层. 构造时在 broker 上注册数据层的消息, 所以同一个频道上的服务器要在同样的位置构造, 并且在 broker.subscribe() 之前.
    // 数据层不自建线程, 存储调用在宿主的 Executor 上, 收到的消息在 Redis 的订阅线程上处理
    public DataServiceImpl(
            @NotNull EntityStorage storage,
            @NotNull Executor executor,
            @NotNull MessageBroker broker,
            @NotNull Duration saveInterval,
            @NotNull Duration snapshotExpiry,
            @Nullable RedisClient redis,
            @Nullable Predicate<String> serverAlive
    ) {
        this.storage = storage;
        this.executor = executor;
        this.messenger = DataMessenger.register(broker);
        this.saveInterval = saveInterval;
        this.snapshotExpiry = snapshotExpiry;
        this.rosters = redis == null ? null : new RosterService(redis, this.messenger, executor, serverAlive);
    }

    // 类型名会拼进 SQL 表名, 限制为 1 到 32 个 [a-z0-9_] 字符. 这里不建表, 表在这个类型第一次要访问存储时才建
    @NotNull
    @Override
    public synchronized <K> EntityType<K> registerEntityType(@NotNull String name, @NotNull EntityIdCodec<K> idCodec) {
        if (!ENTITY_TYPE_NAME.matcher(name).matches()) throw new IllegalArgumentException("Entity type name must be 1 to 32 characters of [a-z0-9_]: " + name);
        if (this.entityTypes.containsKey(name)) throw new IllegalArgumentException("Entity type is already registered: " + name);
        EntityTypeImpl<K> type = new EntityTypeImpl<>(this, name, idCodec);
        this.entityTypes.put(name, type);
        this.messenger.listen(name, type);
        return type;
    }

    @NotNull
    @Override
    public <R> CompletableFuture<R> transaction(@NotNull Function<Transaction, R> body) {
        return CompletableFuture.supplyAsync(() -> new TransactionImpl(this.storage).run(body), this.executor);
    }

    // 停服时调用, 阻塞到全部完成. 对每个类型持有中和正在排队 online / offline 的实体各做一次 offline, 也就是释放再落库,
    // 定时落库到点时看到已释放就停. 全部完成后不再处理收到的消息. 宿主传入的对象在这之后由宿主关闭.
    // 某个实体的 offline 失败时, 其他实体照样做完, 然后把异常抛给调用方. 名单里本服名下的记录在落库之后清掉, 命令连接也在这时关闭
    @Override
    public void close() {
        List<CompletableFuture<Void>> offlines = new ArrayList<>();
        for (EntityTypeImpl<?> type : this.entityTypes.values()) {
            offlines.add(type.residence().offlineAll());
        }
        try {
            CompletableFuture.allOf(offlines.toArray(new CompletableFuture<?>[0])).join();
            if (this.rosters != null) this.rosters.close();
        } finally {
            this.messenger.unlistenAll();
            for (EntityTypeImpl<?> type : this.entityTypes.values()) {
                type.watches().clear();
            }
        }
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
    public DataMessenger messenger() {
        return this.messenger;
    }

    // 宿主没有在 builder 里传 redis 时抛 IllegalStateException
    @NotNull
    public RosterService rosters() {
        if (this.rosters == null) throw new IllegalStateException("Roster keys need a Redis client, pass one to SparrowDataService.Builder#redis");
        return this.rosters;
    }

    @NotNull
    public Duration saveInterval() {
        return this.saveInterval;
    }

    @NotNull
    public Duration snapshotExpiry() {
        return this.snapshotExpiry;
    }
}