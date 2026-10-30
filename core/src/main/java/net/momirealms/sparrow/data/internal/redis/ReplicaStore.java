package net.momirealms.sparrow.data.internal.redis;

import io.lettuce.core.KeyValue;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import net.momirealms.sparrow.data.storage.EntityRef;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@ApiStatus.Internal
public final class ReplicaStore {
    private static final byte[] PREFIX = "sparrow:replica:".getBytes(StandardCharsets.UTF_8);
    private static final byte SEPARATOR = (byte) 0xFF;
    private static final byte[] PRESENT = {(byte) 0xFF};
    private static final byte[] EMPTY = new byte[0];

    private final RedisAsyncCommands<byte[], byte[]> commands;
    private final long ttlMillis;
    private final SetArgs setArgs;

    // Redis 副本的读写. 所有命令走宿主传入的同一条连接, 同一条连接上的命令按发送顺序执行.
    // 读到 null 表示没有副本, 读到空数组表示持有者发布的是"没有值". NBT 编码的值不会是空的, 两者不会混淆
    public ReplicaStore(@NotNull StatefulRedisConnection<byte[], byte[]> connection, @NotNull Duration ttl) {
        this.commands = connection.async();
        this.ttlMillis = ttl.toMillis();
        this.setArgs = SetArgs.Builder.px(this.ttlMillis);
    }

    // 读普通键的副本
    @NotNull
    public CompletableFuture<byte @Nullable []> readValue(@NotNull EntityRef entity, @NotNull String keyId) {
        return this.commands.get(key(entity, keyId)).toCompletableFuture();
    }

    // 读映射键的一个条目, 和标记字段一起取, 才能分清"没有副本"和"副本里没有这个条目"
    @NotNull
    public CompletableFuture<byte @Nullable []> readEntry(@NotNull EntityRef entity, @NotNull String keyId, @NotNull String entry) {
        return this.commands.hmget(key(entity, keyId), PRESENT, entry.getBytes(StandardCharsets.UTF_8)).toCompletableFuture().thenApply(values -> {
            if (!values.get(0).hasValue()) return null;
            KeyValue<byte[], byte[]> value = values.get(1);
            return value.hasValue() ? value.getValue() : EMPTY;
        });
    }

    // 读映射键的全部条目, 没有副本时为 null. 有副本时 Hash 里一定有标记字段, 所以空的结果就是没有副本
    @NotNull
    public CompletableFuture<@Nullable Map<String, byte[]>> readEntries(@NotNull EntityRef entity, @NotNull String keyId) {
        return this.commands.hgetall(key(entity, keyId)).toCompletableFuture().thenApply(fields -> {
            if (fields.isEmpty()) return null;
            Map<String, byte[]> entries = new HashMap<>(fields.size());
            for (Map.Entry<byte[], byte[]> field : fields.entrySet()) {
                if (Arrays.equals(field.getKey(), PRESENT)) continue;
                entries.put(new String(field.getKey(), StandardCharsets.UTF_8), field.getValue());
            }
            return entries;
        });
    }

    // value 为 null 时写入空值, 表示持有者发布的是没有值
    @NotNull
    public CompletableFuture<Void> writeValue(@NotNull EntityRef entity, @NotNull String keyId, byte @Nullable [] value) {
        return this.commands.set(key(entity, keyId), value == null ? EMPTY : value, this.setArgs)
                .toCompletableFuture()
                .thenApply(reply -> null);
    }

    // 写入改过的条目, 删除被删的条目, 再刷新 TTL. 每次都写上标记字段 PRESENT, 条目删光了 Hash 也还在.
    // 三条命令依次执行, 中途被读到的半新半旧状态由随后的失效消息纠正
    @NotNull
    public CompletableFuture<Void> writeEntries(@NotNull EntityRef entity, @NotNull String keyId, @NotNull Map<String, byte[]> changed, @NotNull Collection<String> removed) {
        byte[] key = key(entity, keyId);
        Map<byte[], byte[]> fields = new HashMap<>(changed.size() + 1);
        fields.put(PRESENT, EMPTY);
        for (Map.Entry<String, byte[]> entry : changed.entrySet()) {
            fields.put(entry.getKey().getBytes(StandardCharsets.UTF_8), entry.getValue());
        }
        CompletableFuture<Long> written = this.commands.hset(key, fields).toCompletableFuture();
        CompletableFuture<Long> deleted = removed.isEmpty() ? CompletableFuture.completedFuture(0L) : this.commands.hdel(key, fieldsOf(removed)).toCompletableFuture();
        CompletableFuture<Boolean> expired = this.commands.pexpire(key, this.ttlMillis).toCompletableFuture();
        return CompletableFuture.allOf(written, deleted, expired);
    }

    // 返回是否真的删掉了副本
    @NotNull
    public CompletableFuture<Boolean> delete(@NotNull EntityRef entity, @NotNull String keyId) {
        return this.commands.del(key(entity, keyId)).toCompletableFuture().thenApply(count -> count > 0);
    }

    // sparrow:replica:{类型名} 0xFF {实体 ID} 0xFF {键 ID}. 类型名和键 ID 是 UTF-8 文本, 不会含 0xFF 这个字节,
    // 所以第一个 0xFF 之前是类型名, 最后一个 0xFF 之后是键 ID, 中间剩下的都是实体 ID, 实体 ID 里有 0xFF 也不会混淆
    private static byte @NotNull [] key(@NotNull EntityRef entity, @NotNull String keyId) {
        byte[] type = entity.entityType().getBytes(StandardCharsets.UTF_8);
        byte[] id = entity.entityId();
        byte[] keyBytes = keyId.getBytes(StandardCharsets.UTF_8);
        byte[] key = new byte[PREFIX.length + type.length + 1 + id.length + 1 + keyBytes.length];
        ByteBuffer.wrap(key).put(PREFIX).put(type).put(SEPARATOR).put(id).put(SEPARATOR).put(keyBytes);
        return key;
    }

    private static byte @NotNull [][] fieldsOf(@NotNull Collection<String> entries) {
        byte[][] fields = new byte[entries.size()][];
        int index = 0;
        for (String entry : entries) {
            fields[index++] = entry.getBytes(StandardCharsets.UTF_8);
        }
        return fields;
    }
}