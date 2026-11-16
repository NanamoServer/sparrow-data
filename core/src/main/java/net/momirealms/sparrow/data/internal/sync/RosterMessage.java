package net.momirealms.sparrow.data.internal.sync;

import io.netty.buffer.ByteBuf;
import net.momirealms.sparrow.message.MessageIdentifier;
import net.momirealms.sparrow.message.message.OneWayMessage;
import net.momirealms.sparrow.message.util.ByteBufHelper;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public final class RosterMessage extends OneWayMessage {
    static final MessageIdentifier ID = MessageIdentifier.of(DataMessenger.NAMESPACE, "roster");

    private final DataMessenger messenger;
    private final RosterChange change;

    // 名单的变化由 Redis 脚本和哈希的写入一起发布, 所有服务器包括发出方自己都按 Redis 里的顺序收到
    RosterMessage(@NotNull DataMessenger messenger, @NotNull RosterChange change) {
        this.messenger = messenger;
        this.change = change;
    }

    RosterMessage(@NotNull DataMessenger messenger, @NotNull ByteBuf buf) {
        this.messenger = messenger;
        this.change = new RosterChange(
                ByteBufHelper.readUtf8(buf, DataMessenger.MAX_TEXT_LENGTH),
                ByteBufHelper.readUtf8(buf, DataMessenger.MAX_TEXT_LENGTH),
                RosterChange.Kind.byOrdinal(buf.readByte()),
                ByteBufHelper.readUtf8(buf, DataMessenger.MAX_TEXT_LENGTH),
                buf.readLong(),
                Utils.readNullableByteArray(buf, DataMessenger.MAX_ENTITY_ID_LENGTH),
                Utils.readNullableByteArray(buf, Integer.MAX_VALUE)
        );
    }

    void write(@NotNull ByteBuf buf) {
        ByteBufHelper.writeUtf8(buf, this.change.entityType(), DataMessenger.MAX_TEXT_LENGTH);
        ByteBufHelper.writeUtf8(buf, this.change.keyId(), DataMessenger.MAX_TEXT_LENGTH);
        buf.writeByte(this.change.kind().ordinal());
        ByteBufHelper.writeUtf8(buf, this.change.serverId(), DataMessenger.MAX_TEXT_LENGTH);
        buf.writeLong(this.change.sequence());
        Utils.writeNullableByteArray(buf, this.change.entityId());
        Utils.writeNullableByteArray(buf, this.change.value());
    }

    @Override
    public void handle() {
        this.messenger.receive(this.change);
    }

    @NotNull
    @Override
    public MessageIdentifier identifier() {
        return ID;
    }
}