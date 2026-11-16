package net.momirealms.sparrow.data.internal.sync;

import io.netty.buffer.ByteBuf;
import net.momirealms.sparrow.message.MessageIdentifier;
import net.momirealms.sparrow.message.message.OneWayMessage;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public final class RosterProbeMessage extends OneWayMessage {
    static final MessageIdentifier ID = MessageIdentifier.of(DataMessenger.NAMESPACE, "roster_probe");

    private final DataMessenger messenger;
    private final long nonce;

    // 本服发给自己的探测. 它必须绕一圈 Redis 再从订阅连接回来, 收到了才说明订阅已经恢复
    RosterProbeMessage(@NotNull DataMessenger messenger, long nonce) {
        this.messenger = messenger;
        this.nonce = nonce;
    }

    RosterProbeMessage(@NotNull DataMessenger messenger, @NotNull ByteBuf buf) {
        this.messenger = messenger;
        this.nonce = buf.readLong();
    }

    void write(@NotNull ByteBuf buf) {
        buf.writeLong(this.nonce);
    }

    @Override
    public void handle() {
        this.messenger.receiveProbe(this.nonce);
    }

    @Override
    public boolean shortCircuitLocally() {
        return false;
    }

    @NotNull
    @Override
    public MessageIdentifier identifier() {
        return ID;
    }
}