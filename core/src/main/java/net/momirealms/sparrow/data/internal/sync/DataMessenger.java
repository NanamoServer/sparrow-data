package net.momirealms.sparrow.data.internal.sync;

import io.netty.buffer.ByteBuf;
import net.momirealms.sparrow.data.storage.StoredValue;
import net.momirealms.sparrow.redis.messagebroker.MessageBroker;
import net.momirealms.sparrow.redis.messagebroker.RedisMessage;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ApiStatus.Internal
public final class DataMessenger<B extends ByteBuf> {
    static final String NAMESPACE = "sparrow_data";
    static final int MAX_TEXT_LENGTH = 32767;
    static final int MAX_ENTITY_ID_LENGTH = 64;
    private static final String BROADCAST = "";

    private final MessageBroker<B> broker;
    private final Map<String, Listener> listeners = new ConcurrentHashMap<>();

    private DataMessenger(@NotNull MessageBroker<B> broker) {
        this.broker = broker;
    }

    // 在宿主的 broker 上注册失效和覆盖消息. 宿主传入的是 MessageBroker<? extends ByteBuf>, 调用这个泛型方法时通配符被捕获成 B,
    // 消息类和编解码器都按 B 写就能注册上去. broker 按注册顺序给消息编号, 所以同一个频道上的服务器都要在同样的位置注册,
    // 并且在 broker.subscribe() 之前注册
    @NotNull
    public static <B extends ByteBuf> DataMessenger<B> register(@NotNull MessageBroker<B> broker) {
        DataMessenger<B> messenger = new DataMessenger<>(broker);
        broker.registry().register(InvalidateMessage.ID, RedisMessage.codec(InvalidateMessage::write, buf -> new InvalidateMessage<>(messenger, buf)));
        broker.registry().register(OverrideMessage.ID, RedisMessage.codec(OverrideMessage::write, buf -> new OverrideMessage<>(messenger, buf)));
        return messenger;
    }

    public void listen(@NotNull String entityType, @NotNull Listener listener) {
        this.listeners.put(entityType, listener);
    }

    // 广播给所有服务器, 包括本服自己. 范围的含义见 InvalidateMessage 的构造器
    public void invalidate(@NotNull String entityType, byte @Nullable [] entityId, @Nullable String keyId, @Nullable String entry) {
        this.broker.publishOneWay(new InvalidateMessage<>(this, entityType, entityId, keyId, entry), BROADCAST);
    }

    public void override(@NotNull String entityType, byte @NotNull [] entityId, @NotNull String keyId, @Nullable String entry, @Nullable StoredValue value) {
        this.broker.publishOneWay(new OverrideMessage<>(this, entityType, entityId, keyId, entry, value), BROADCAST);
    }

    // broker 在 Redis 的订阅线程上调用. 这里只按实体类型找到监听者, 本服没有这个实体类型时忽略
    void receive(@NotNull InvalidateMessage<B> message) {
        Listener listener = this.listeners.get(message.entityType());
        if (listener != null) {
            listener.invalidate(message.entityId(), message.keyId(), message.entry());
        }
    }

    void receive(@NotNull OverrideMessage<B> message) {
        Listener listener = this.listeners.get(message.entityType());
        if (listener != null) {
            listener.override(message.entityId(), message.keyId(), message.entry(), message.value());
        }
    }

    // 一个实体类型的消息接收方. 在 Redis 的订阅线程上调用, 这个线程卡住会让所有消息排队, 所以实现里只做分发, 重读交给数据层自己的线程
    public interface Listener {

        void invalidate(byte @Nullable [] entityId, @Nullable String keyId, @Nullable String entry);

        void override(byte @NotNull [] entityId, @NotNull String keyId, @Nullable String entry, @Nullable StoredValue value);
    }
}