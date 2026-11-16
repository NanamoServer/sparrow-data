package net.momirealms.sparrow.data.internal.roster;

import io.lettuce.core.RedisChannelHandler;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisConnectionStateListener;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import net.momirealms.sparrow.data.internal.key.RosterKeyImpl;
import net.momirealms.sparrow.data.internal.sync.DataMessenger;
import net.momirealms.sparrow.data.internal.sync.RosterChange;
import net.momirealms.sparrow.message.PreparedMessage;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

@ApiStatus.Internal
public final class RosterService implements DataMessenger.RosterListener, RedisConnectionStateListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(RosterService.class);
    private static final long PROBE_RETRY_SECONDS = 1;
    // 写入记录并发布变化
    private static final String ANNOUNCE_SCRIPT = """
            redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
            redis.call('PUBLISH', ARGV[3], ARGV[4])
            return 1
            """;
    // 记录以本服的归属前缀开头时才删除并发布, 别的服务器接管后的记录保持原样
    private static final String WITHDRAW_SCRIPT = """
            local current = redis.call('HGET', KEYS[1], ARGV[1])
            if not current or string.sub(current, 1, #ARGV[2]) ~= ARGV[2] then
                return 0
            end
            redis.call('HDEL', KEYS[1], ARGV[1])
            redis.call('PUBLISH', ARGV[3], ARGV[4])
            return 1
            """;
    // 删掉本服名下的全部记录并发布
    private static final String RESET_SCRIPT = """
            local entries = redis.call('HGETALL', KEYS[1])
            for i = 1, #entries, 2 do
                if string.sub(entries[i + 1], 1, #ARGV[1]) == ARGV[1] then
                    redis.call('HDEL', KEYS[1], entries[i])
                end
            end
            redis.call('PUBLISH', ARGV[2], ARGV[3])
            return 1
            """;
    // 记录不存在时才写入并发布
    private static final String RESTORE_SCRIPT = """
            if redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2]) == 1 then
                redis.call('PUBLISH', ARGV[3], ARGV[4])
            end
            return 1
            """;

    private final DataMessenger messenger;
    private final RedisClient client;
    private final StatefulRedisConnection<byte[], byte[]> connection;
    private final RedisAsyncCommands<byte[], byte[]> commands;
    private final Executor probeRetry;
    private final @Nullable Predicate<String> serverAlive;
    private final String serverId;
    private final byte[] ownerPrefix; // 本服写进记录开头的归属前缀
    private final Map<String, Roster<?, ?>> rosters = new ConcurrentHashMap<>(); // 实体类型名和键 ID 拼成的名字 -> 名单
    private final AtomicLong sequence = new AtomicLong(System.nanoTime() & Long.MAX_VALUE); // 不从 0 开始, 上次运行迟到的回显对不上号

    private boolean resyncing;   // 一轮对账从发出探测开始, 到全部名单重新装载完结束
    private boolean resyncAgain; // 对账期间连接又恢复过一次, 这一轮结束后再做一轮
    private long awaitedProbe;   // 正在等的探测编号, 0 表示没在等
    private boolean closed;

    // 名单键在 Redis 上的读写和重连后的对账. 在宿主的 client 上自己开一条命令连接, 命令都是异步提交, 不占线程.
    // 订阅断开期间的消息会丢, 所以 client 上任何连接恢复之后都对一次账: 确认订阅恢复, 补回本服的记录, 重新装载全部镜像
    public RosterService(@NotNull RedisClient client, @NotNull DataMessenger messenger, @NotNull Executor executor, @Nullable Predicate<String> serverAlive) {
        this.messenger = messenger;
        this.client = client;
        this.connection = client.connect(ByteArrayCodec.INSTANCE);
        this.commands = this.connection.async();
        this.probeRetry = CompletableFuture.delayedExecutor(PROBE_RETRY_SECONDS, TimeUnit.SECONDS, executor);
        this.serverAlive = serverAlive;
        this.serverId = messenger.serverId();
        this.ownerPrefix = ownerPrefix(this.serverId);
        messenger.listenRosters(this);
        client.addListener(this);
    }

    // 先放进路由表再装载, 装载期间收到的消息由名单自己暂存
    @NotNull
    public <K, V> CompletableFuture<Void> open(@NotNull RosterKeyImpl<K, V> key) {
        Roster<K, V> roster = new Roster<>(this, key);
        key.roster(roster);
        this.rosters.put(name(key.type().name(), key.storageKey().id()), roster);
        return roster.open();
    }

    // 注销名单键时调用. 清掉本服名下的记录, 观察它的 watch 通知最后一次再关闭
    @NotNull
    public CompletableFuture<Void> close(@NotNull RosterKeyImpl<?, ?> key) {
        this.rosters.remove(name(key.type().name(), key.storageKey().id()));
        CompletableFuture<Long> cleared = key.roster().clearOwned();
        key.roster().closeWatches(true);
        return cleared.thenApply(ignored -> null);
    }

    // 数据层关闭时调用, 阻塞到本服名下的记录全部清掉. 之后不再对账, 命令连接在这里关闭, client 留给宿主
    public void close() {
        synchronized (this) {
            this.closed = true;
        }
        this.client.removeListener(this);
        List<CompletableFuture<Long>> cleared = new ArrayList<>();
        for (Roster<?, ?> roster : this.rosters.values()) {
            cleared.add(roster.clearOwned());
            roster.closeWatches(false);
        }
        this.rosters.clear();
        try {
            CompletableFuture.allOf(cleared.toArray(new CompletableFuture<?>[0])).join();
        } finally {
            this.connection.close();
        }
    }

    @Override
    public void change(@NotNull RosterChange change) {
        Roster<?, ?> roster = this.rosters.get(name(change.entityType(), change.keyId()));
        if (roster != null) {
            roster.receive(change);
        }
    }

    // 收到自己发的探测, 订阅已经恢复, 这之后发布的变化都收得到. 现在读到的全量加上之后的消息就是完整的名单
    @Override
    public void probe(long nonce) {
        List<CompletableFuture<Void>> loading = new ArrayList<>();
        synchronized (this) {
            if (this.closed || nonce != this.awaitedProbe) return;

            this.awaitedProbe = 0;
            for (Roster<?, ?> roster : this.rosters.values()) {
                loading.add(roster.resync());
            }
        }
        CompletableFuture.allOf(loading.toArray(new CompletableFuture<?>[0])).whenComplete((ignored, error) -> {
            if (error != null) {
                LOGGER.error("Failed to resync rosters after a Redis reconnect", error);
            }
            this.finishResync();
        });
    }

    // client 上任何一条连接建立或恢复时都会调用, 包括宿主自己的连接. 分不清是不是订阅连接, 一律对一次账
    @Override
    public void onRedisConnected(RedisChannelHandler<?, ?> connection, SocketAddress socketAddress) {
        synchronized (this) {
            if (this.closed || this.rosters.isEmpty()) return;
            if (this.resyncing) {
                this.resyncAgain = true;
                return;
            }
            this.resyncing = true;
            this.sendProbe();
        }
    }

    private void finishResync() {
        synchronized (this) {
            this.resyncing = false;
            if (this.closed || !this.resyncAgain) return;

            this.resyncAgain = false;
            this.resyncing = true;
            this.sendProbe();
        }
    }

    // 探测要绕一圈 Redis 从订阅连接回来. 订阅还没恢复时发出的探测收不到, 到时间没收到就重发. 调用方持有实例锁
    private void sendProbe() {
        long nonce = this.sequence.incrementAndGet();
        this.awaitedProbe = nonce;
        PreparedMessage probe = this.messenger.prepareProbe(nonce);
        this.commands.publish(probe.channel().getBytes(StandardCharsets.UTF_8), probe.payload());
        this.probeRetry.execute(() -> {
            synchronized (this) {
                if (this.closed || this.awaitedProbe != nonce) return;

                this.sendProbe();
            }
        });
    }

    long nextSequence() {
        return this.sequence.incrementAndGet();
    }

    @NotNull
    String serverId() {
        return this.serverId;
    }

    // 本服自己的记录不问宿主, 本服一定活着
    boolean alive(@NotNull String serverId) {
        return this.serverAlive == null || serverId.equals(this.serverId) || this.serverAlive.test(serverId);
    }

    @NotNull
    CompletableFuture<Long> announce(byte @NotNull [] redisKey, byte @NotNull [] field, byte @NotNull [] value, @NotNull RosterChange change) {
        PreparedMessage message = this.messenger.prepare(change);
        return this.evaluate(ANNOUNCE_SCRIPT, redisKey, field, this.record(value), channel(message), message.payload());
    }

    // 返回 1 表示删掉了记录并发布了变化, 0 表示记录不存在或者不归本服
    @NotNull
    CompletableFuture<Long> withdraw(byte @NotNull [] redisKey, byte @NotNull [] field, @NotNull RosterChange change) {
        PreparedMessage message = this.messenger.prepare(change);
        return this.evaluate(WITHDRAW_SCRIPT, redisKey, field, this.ownerPrefix, channel(message), message.payload());
    }

    @NotNull
    CompletableFuture<Long> reset(byte @NotNull [] redisKey, @NotNull RosterChange change) {
        PreparedMessage message = this.messenger.prepare(change);
        return this.evaluate(RESET_SCRIPT, redisKey, this.ownerPrefix, channel(message), message.payload());
    }

    @NotNull
    CompletableFuture<Long> restore(byte @NotNull [] redisKey, byte @NotNull [] field, byte @NotNull [] value, @NotNull RosterChange change) {
        PreparedMessage message = this.messenger.prepare(change);
        return this.evaluate(RESTORE_SCRIPT, redisKey, field, this.record(value), channel(message), message.payload());
    }

    @NotNull
    CompletableFuture<Map<byte[], byte[]>> readAll(byte @NotNull [] redisKey) {
        return this.commands.hgetall(redisKey).toCompletableFuture();
    }

    // 一个名单在 Redis 里是一个哈希, 键里带上 appId, 同一个 Redis 上的不同群组互不干扰
    byte @NotNull [] redisKey(@NotNull String entityType, @NotNull String keyId) {
        return (this.messenger.appId() + ":sparrow_data:roster:" + entityType + ":" + keyId).getBytes(StandardCharsets.UTF_8);
    }

    // 命令在同一条连接上按提交顺序执行, 名单靠这个顺序保证先提交的写入先生效
    @NotNull
    private CompletableFuture<Long> evaluate(@NotNull String script, byte @NotNull [] redisKey, byte[]... arguments) {
        return this.commands.<Long>eval(script, ScriptOutputType.INTEGER, new byte[][]{redisKey}, arguments).toCompletableFuture();
    }

    // 记录是归属前缀接上编码后的值
    private byte @NotNull [] record(byte @NotNull [] value) {
        byte[] record = Arrays.copyOf(this.ownerPrefix, this.ownerPrefix.length + value.length);
        System.arraycopy(value, 0, record, this.ownerPrefix.length, value.length);
        return record;
    }

    // 归属前缀是两个字节的长度接上服务器 ID. 带着长度比较, "s1" 不会被当成 "s10" 的前缀
    private static byte @NotNull [] ownerPrefix(@NotNull String serverId) {
        byte[] id = serverId.getBytes(StandardCharsets.UTF_8);
        byte[] prefix = new byte[id.length + 2];
        prefix[0] = (byte) (id.length >>> 8);
        prefix[1] = (byte) id.length;
        System.arraycopy(id, 0, prefix, 2, id.length);
        return prefix;
    }

    @NotNull
    static String owner(byte @NotNull [] record) {
        return new String(record, 2, ownerLength(record), StandardCharsets.UTF_8);
    }

    static byte @NotNull [] value(byte @NotNull [] record) {
        return Arrays.copyOfRange(record, 2 + ownerLength(record), record.length);
    }

    private static int ownerLength(byte @NotNull [] record) {
        return ((record[0] & 0xFF) << 8) | (record[1] & 0xFF);
    }

    private static byte @NotNull [] channel(@NotNull PreparedMessage message) {
        return message.channel().getBytes(StandardCharsets.UTF_8);
    }

    // 键 ID 和类型名都不含换行, 用它分隔
    @NotNull
    private static String name(@NotNull String entityType, @NotNull String keyId) {
        return entityType + '\n' + keyId;
    }
}