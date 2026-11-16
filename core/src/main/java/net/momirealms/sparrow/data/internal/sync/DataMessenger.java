package net.momirealms.sparrow.data.internal.sync;

import net.momirealms.sparrow.data.storage.StoredValue;
import net.momirealms.sparrow.message.MessageBroker;
import net.momirealms.sparrow.message.PreparedMessage;
import net.momirealms.sparrow.message.Target;
import net.momirealms.sparrow.message.codec.MessageCodec;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ApiStatus.Internal
public final class DataMessenger {
    static final String NAMESPACE = "sparrow_data";
    static final int MAX_TEXT_LENGTH = 32767;
    static final int MAX_ENTITY_ID_LENGTH = 64;

    private final MessageBroker broker;
    private final Map<String, Listener> listeners = new ConcurrentHashMap<>();
    private volatile @Nullable RosterListener rosters;

    private DataMessenger(@NotNull MessageBroker broker) {
        this.broker = broker;
    }

    // 同一频道按固定顺序注册数据消息, 注册须在订阅之前完成.
    @NotNull
    public static DataMessenger register(@NotNull MessageBroker broker) {
        DataMessenger messenger = new DataMessenger(broker);
        broker.registry().register(InvalidateMessage.ID, MessageCodec.of(InvalidateMessage::write, buf -> new InvalidateMessage(messenger, buf)));
        broker.registry().register(OverrideMessage.ID, MessageCodec.of(OverrideMessage::write, buf -> new OverrideMessage(messenger, buf)));
        broker.registry().register(PublishMessage.ID, MessageCodec.of(PublishMessage::write, buf -> new PublishMessage(messenger, buf)));
        broker.registry().register(RosterMessage.ID, MessageCodec.of(RosterMessage::write, buf -> new RosterMessage(messenger, buf)));
        broker.registry().register(RosterProbeMessage.ID, MessageCodec.of(RosterProbeMessage::write, buf -> new RosterProbeMessage(messenger, buf)));
        return messenger;
    }

    @NotNull
    public String appId() {
        return this.broker.appId();
    }

    @NotNull
    public String serverId() {
        return this.broker.serverId();
    }

    public void listenRosters(@NotNull RosterListener listener) {
        this.rosters = listener;
    }

    // 编码成频道和字节, 由调用方在 Redis 脚本里和哈希的写入一起发布. 发给所有服务器, 包括本服
    @NotNull
    public PreparedMessage prepare(@NotNull RosterChange change) {
        return this.broker.prepare(new RosterMessage(this, change), Target.all());
    }

    // 只发给本服的探测, 同样由调用方自己发布到 Redis
    @NotNull
    public PreparedMessage prepareProbe(long nonce) {
        return this.broker.prepare(new RosterProbeMessage(this, nonce), Target.server(this.broker.serverId()));
    }

    public void listen(@NotNull String entityType, @NotNull Listener listener) {
        this.listeners.put(entityType, listener);
    }

    // DataService.close 的最后调用, 之后收到的消息都忽略. 消息类型还留在宿主的 broker 上, 编号不变
    public void unlistenAll() {
        this.listeners.clear();
        this.rosters = null;
    }

    // 广播给所有服务器, 包括本服自己. 范围的含义见 InvalidateMessage 的构造器
    public void invalidate(@NotNull String entityType, byte @Nullable [] entityId, @Nullable String keyId, @Nullable String entry) {
        this.broker.publishAll(new InvalidateMessage(this, entityType, entityId, keyId, entry));
    }

    // 视图键失效时用, 本服自己已经发起重读, 只通知别的服
    public void invalidateOthers(@NotNull String entityType, byte @NotNull [] entityId, @NotNull String keyId) {
        this.broker.publishOthers(new InvalidateMessage(this, entityType, entityId, keyId, null));
    }

    public void override(@NotNull String entityType, byte @NotNull [] entityId, @NotNull String keyId, @Nullable String entry, @Nullable StoredValue value) {
        this.broker.publishAll(new OverrideMessage(this, entityType, entityId, keyId, entry, value));
    }

    // value 为空表示 publish 的是"没有值"
    public void publishValue(@NotNull String entityType, byte @NotNull [] entityId, @NotNull String keyId, @Nullable StoredValue value) {
        this.broker.publishAll(new PublishMessage(this, entityType, entityId, keyId, value, null));
    }

    public void publishEntries(@NotNull String entityType, byte @NotNull [] entityId, @NotNull String keyId, @NotNull Map<String, StoredValue> entries) {
        this.broker.publishAll(new PublishMessage(this, entityType, entityId, keyId, null, entries));
    }

    // broker 在 Redis 的订阅线程上调用. 这里只按实体类型找到监听者, 本服没有这个实体类型时忽略
    void receive(@NotNull InvalidateMessage message) {
        Listener listener = this.listeners.get(message.entityType());
        if (listener != null) {
            listener.invalidate(message.entityId(), message.keyId(), message.entry());
        }
    }

    void receive(@NotNull OverrideMessage message) {
        Listener listener = this.listeners.get(message.entityType());
        if (listener != null) {
            listener.override(message.entityId(), message.keyId(), message.entry(), message.value());
        }
    }

    void receive(@NotNull PublishMessage message) {
        Listener listener = this.listeners.get(message.entityType());
        if (listener == null) return;
        Map<String, StoredValue> entries = message.entries();
        if (entries == null) {
            listener.publishValue(message.entityId(), message.keyId(), message.value());
        } else {
            listener.publishEntries(message.entityId(), message.keyId(), entries);
        }
    }

    void receive(@NotNull RosterChange change) {
        RosterListener listener = this.rosters;
        if (listener != null) {
            listener.change(change);
        }
    }

    void receiveProbe(long nonce) {
        RosterListener listener = this.rosters;
        if (listener != null) {
            listener.probe(nonce);
        }
    }

    // 名单消息的接收方, 在 Redis 的订阅线程上按到达顺序调用, 不能阻塞
    public interface RosterListener {

        void change(@NotNull RosterChange change);

        void probe(long nonce);
    }

    // 一个实体类型的消息接收方, 在 Redis 的订阅线程上直接处理. 解码和写进内存都只要微秒, 读库这类要等的事交给宿主的 Executor
    public interface Listener {

        void invalidate(byte @Nullable [] entityId, @Nullable String keyId, @Nullable String entry);

        void override(byte @NotNull [] entityId, @NotNull String keyId, @Nullable String entry, @Nullable StoredValue value);

        void publishValue(byte @NotNull [] entityId, @NotNull String keyId, @Nullable StoredValue value);

        void publishEntries(byte @NotNull [] entityId, @NotNull String keyId, @NotNull Map<String, StoredValue> entries);
    }
}