package net.momirealms.sparrow.data.internal.sync;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * 名单的一次变化, 也是名单消息的内容.
 *
 * @param serverId 发出这次变化的服务器, SET 时也是记录的所属服务器
 * @param sequence 发出方给这次操作编的号, 发出方收到自己的回显时靠它认出是哪一次操作. 不对应某次操作时为 -1
 * @param entityId RESET 时为 null
 * @param value 只有 SET 带值
 */
@ApiStatus.Internal
public record RosterChange(
        @NotNull String entityType,
        @NotNull String keyId,
        @NotNull Kind kind,
        @NotNull String serverId,
        long sequence,
        byte @Nullable [] entityId,
        byte @Nullable [] value
) {

    public enum Kind {
        SET,    // 写入或换成新值, 归属换成 serverId
        REMOVE, // 所属服务器撤回了这条记录
        RESET;  // serverId 名下的记录全部清掉

        private static final Kind[] VALUES = values();

        @NotNull
        static Kind byOrdinal(int ordinal) {
            return VALUES[ordinal];
        }
    }
}