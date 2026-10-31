package net.momirealms.sparrow.data.internal.sync;

import io.netty.buffer.ByteBuf;
import net.momirealms.sparrow.data.storage.StoredValue;
import net.momirealms.sparrow.redis.messagebroker.MessageIdentifier;
import net.momirealms.sparrow.redis.messagebroker.message.OneWayMessage;
import net.momirealms.sparrow.redis.messagebroker.util.ByteBufHelper;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

@ApiStatus.Internal
public final class PublishMessage<B extends ByteBuf> extends OneWayMessage<B> {
    static final MessageIdentifier ID = MessageIdentifier.of(DataMessenger.NAMESPACE, "publish");

    private final DataMessenger<B> messenger;
    private final String entityType;
    private final byte[] entityId;
    private final String keyId;
    private final @Nullable StoredValue value;
    private final @Nullable Map<String, StoredValue> entries;

    // 持有方 publish 一个键的当前值, 不落库, 收到的服直接把值放进快照. entries 不为空时是映射键的全部条目,
    // 否则是普通键的值, value 为空表示没有值
    PublishMessage(
            @NotNull DataMessenger<B> messenger,
            @NotNull String entityType,
            byte @NotNull [] entityId,
            @NotNull String keyId,
            @Nullable StoredValue value,
            @Nullable Map<String, StoredValue> entries
    ) {
        this.messenger = messenger;
        this.entityType = entityType;
        this.entityId = entityId;
        this.keyId = keyId;
        this.value = value;
        this.entries = entries;
    }

    PublishMessage(@NotNull DataMessenger<B> messenger, @NotNull B buf) {
        super(buf);
        this.messenger = messenger;
        this.entityType = ByteBufHelper.readUtf8(buf, DataMessenger.MAX_TEXT_LENGTH);
        this.entityId = ByteBufHelper.readByteArray(buf, DataMessenger.MAX_ENTITY_ID_LENGTH);
        this.keyId = ByteBufHelper.readUtf8(buf, DataMessenger.MAX_TEXT_LENGTH);
        if (buf.readBoolean()) {
            int size = ByteBufHelper.readCompactInt(buf);
            Map<String, StoredValue> entries = HashMap.newHashMap(size);
            for (int i = 0; i < size; i++) {
                entries.put(ByteBufHelper.readUtf8(buf, DataMessenger.MAX_TEXT_LENGTH), ValueCodec.read(buf));
            }
            this.value = null;
            this.entries = entries;
        } else {
            this.value = ValueCodec.read(buf);
            this.entries = null;
        }
    }

    // 先写一个 boolean 表示是不是映射键, 映射键再写条目数和每个条目
    @Override
    protected void write(@NotNull B buf) {
        super.write(buf);
        ByteBufHelper.writeUtf8(buf, this.entityType, DataMessenger.MAX_TEXT_LENGTH);
        ByteBufHelper.writeByteArray(buf, this.entityId);
        ByteBufHelper.writeUtf8(buf, this.keyId, DataMessenger.MAX_TEXT_LENGTH);
        buf.writeBoolean(this.entries != null);
        if (this.entries == null) {
            ValueCodec.write(buf, this.value);
            return;
        }
        ByteBufHelper.writeCompactInt(buf, this.entries.size());
        this.entries.forEach((entry, value) -> {
            ByteBufHelper.writeUtf8(buf, entry, DataMessenger.MAX_TEXT_LENGTH);
            ValueCodec.write(buf, value);
        });
    }

    @Override
    protected void handle() {
        this.messenger.receive(this);
    }

    @NotNull
    @Override
    public MessageIdentifier identifier() {
        return ID;
    }

    @NotNull
    String entityType() {
        return this.entityType;
    }

    byte @NotNull [] entityId() {
        return this.entityId;
    }

    @NotNull
    String keyId() {
        return this.keyId;
    }

    @Nullable
    StoredValue value() {
        return this.value;
    }

    @Nullable
    Map<String, StoredValue> entries() {
        return this.entries;
    }
}