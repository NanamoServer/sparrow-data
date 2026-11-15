package net.momirealms.sparrow.data.internal.sync;

import io.netty.buffer.ByteBuf;
import net.momirealms.sparrow.data.util.ByteBufUtils;
import net.momirealms.sparrow.message.MessageIdentifier;
import net.momirealms.sparrow.message.message.OneWayMessage;
import net.momirealms.sparrow.message.util.ByteBufHelper;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@ApiStatus.Internal
public final class InvalidateMessage extends OneWayMessage {
    static final MessageIdentifier ID = MessageIdentifier.of(DataMessenger.NAMESPACE, "invalidate");

    private final DataMessenger messenger;
    private final String entityType;
    private final byte @Nullable [] entityId;
    private final @Nullable String keyId;
    private final @Nullable String entry;

    // 失效的范围由哪些字段为空决定: entry 不为空是一个条目; entry 为空是整个键; keyId 为空是这个实体的全部键;
    // entityId 为空是这个键在所有实体上. entityId 和 keyId 不会同时为空
    InvalidateMessage(@NotNull DataMessenger messenger, @NotNull String entityType, byte @Nullable [] entityId, @Nullable String keyId, @Nullable String entry) {
        this.messenger = messenger;
        this.entityType = entityType;
        this.entityId = entityId;
        this.keyId = keyId;
        this.entry = entry;
    }

    InvalidateMessage(@NotNull DataMessenger messenger, @NotNull ByteBuf buf) {
        this.messenger = messenger;
        this.entityType = ByteBufHelper.readUtf8(buf, DataMessenger.MAX_TEXT_LENGTH);
        this.entityId = ByteBufUtils.readNullableByteArray(buf, DataMessenger.MAX_ENTITY_ID_LENGTH);
        this.keyId = ByteBufUtils.readNullableUtf8(buf, DataMessenger.MAX_TEXT_LENGTH);
        this.entry = ByteBufUtils.readNullableUtf8(buf, DataMessenger.MAX_TEXT_LENGTH);
    }

    void write(@NotNull ByteBuf buf) {
        ByteBufHelper.writeUtf8(buf, this.entityType, DataMessenger.MAX_TEXT_LENGTH);
        ByteBufUtils.writeNullableByteArray(buf, this.entityId);
        ByteBufUtils.writeNullableUtf8(buf, this.keyId, DataMessenger.MAX_TEXT_LENGTH);
        ByteBufUtils.writeNullableUtf8(buf, this.entry, DataMessenger.MAX_TEXT_LENGTH);
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

    byte @Nullable [] entityId() {
        return this.entityId;
    }

    @Nullable
    String keyId() {
        return this.keyId;
    }

    @Nullable
    String entry() {
        return this.entry;
    }
}