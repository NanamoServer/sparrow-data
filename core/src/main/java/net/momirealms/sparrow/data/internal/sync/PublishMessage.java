package net.momirealms.sparrow.data.internal.sync;

import io.netty.buffer.ByteBuf;
import net.momirealms.sparrow.data.storage.StoredValue;
import net.momirealms.sparrow.message.MessageIdentifier;
import net.momirealms.sparrow.message.message.OneWayMessage;
import net.momirealms.sparrow.message.util.ByteBufHelper;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@ApiStatus.Internal
public final class PublishMessage extends OneWayMessage {
    static final MessageIdentifier ID = MessageIdentifier.of(DataMessenger.NAMESPACE, "publish");

    private final DataMessenger messenger;
    private final String entityType;
    private final byte[] entityId;
    private final String keyId;
    private final @Nullable StoredValue value;

    // 持有方 publish 一个键的当前值, 不落库, 收到的服直接把值放进快照. value 为空表示没有值
    PublishMessage(@NotNull DataMessenger messenger, @NotNull String entityType, byte @NotNull [] entityId, @NotNull String keyId, @Nullable StoredValue value) {
        this.messenger = messenger;
        this.entityType = entityType;
        this.entityId = entityId;
        this.keyId = keyId;
        this.value = value;
    }

    PublishMessage(@NotNull DataMessenger messenger, @NotNull ByteBuf buf) {
        this.messenger = messenger;
        this.entityType = ByteBufHelper.readUtf8(buf, DataMessenger.MAX_TEXT_LENGTH);
        this.entityId = ByteBufHelper.readByteArray(buf, DataMessenger.MAX_ENTITY_ID_LENGTH);
        this.keyId = ByteBufHelper.readUtf8(buf, DataMessenger.MAX_TEXT_LENGTH);
        this.value = StoredValueCodec.read(buf);
    }

    void write(@NotNull ByteBuf buf) {
        ByteBufHelper.writeUtf8(buf, this.entityType, DataMessenger.MAX_TEXT_LENGTH);
        ByteBufHelper.writeByteArray(buf, this.entityId);
        ByteBufHelper.writeUtf8(buf, this.keyId, DataMessenger.MAX_TEXT_LENGTH);
        StoredValueCodec.write(buf, this.value);
    }

    @Override
    public void handle() {
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
}