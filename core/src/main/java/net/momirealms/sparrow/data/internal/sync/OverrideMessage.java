package net.momirealms.sparrow.data.internal.sync;

import io.netty.buffer.ByteBuf;
import net.momirealms.sparrow.data.util.ByteBufUtils;
import net.momirealms.sparrow.data.storage.StoredValue;
import net.momirealms.sparrow.redis.messagebroker.MessageIdentifier;
import net.momirealms.sparrow.redis.messagebroker.message.OneWayMessage;
import net.momirealms.sparrow.redis.messagebroker.util.ByteBufHelper;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@ApiStatus.Internal
public final class OverrideMessage<B extends ByteBuf> extends OneWayMessage<B> {
    static final MessageIdentifier ID = MessageIdentifier.of(DataMessenger.NAMESPACE, "override");

    private final DataMessenger<B> messenger;
    private final String entityType;
    private final byte[] entityId;
    private final String keyId;
    private final @Nullable String entry;
    private final @Nullable StoredValue value;

    // 覆盖一个键或一个条目. value 为空表示重置, 持有者删掉这个值或条目
    OverrideMessage(@NotNull DataMessenger<B> messenger, @NotNull String entityType, byte @NotNull [] entityId, @NotNull String keyId, @Nullable String entry, @Nullable StoredValue value) {
        this.messenger = messenger;
        this.entityType = entityType;
        this.entityId = entityId;
        this.keyId = keyId;
        this.entry = entry;
        this.value = value;
    }

    OverrideMessage(@NotNull DataMessenger<B> messenger, @NotNull B buf) {
        super(buf);
        this.messenger = messenger;
        this.entityType = ByteBufHelper.readUtf8(buf, DataMessenger.MAX_TEXT_LENGTH);
        this.entityId = ByteBufHelper.readByteArray(buf, DataMessenger.MAX_ENTITY_ID_LENGTH);
        this.keyId = ByteBufHelper.readUtf8(buf, DataMessenger.MAX_TEXT_LENGTH);
        this.entry = ByteBufUtils.readNullableUtf8(buf, DataMessenger.MAX_TEXT_LENGTH);
        this.value = StoredValueCodec.read(buf);
    }

    @Override
    protected void write(@NotNull B buf) {
        super.write(buf);
        ByteBufHelper.writeUtf8(buf, this.entityType, DataMessenger.MAX_TEXT_LENGTH);
        ByteBufHelper.writeByteArray(buf, this.entityId);
        ByteBufHelper.writeUtf8(buf, this.keyId, DataMessenger.MAX_TEXT_LENGTH);
        ByteBufUtils.writeNullableUtf8(buf, this.entry, DataMessenger.MAX_TEXT_LENGTH);
        StoredValueCodec.write(buf, this.value);
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
    String entry() {
        return this.entry;
    }

    @Nullable
    StoredValue value() {
        return this.value;
    }
}