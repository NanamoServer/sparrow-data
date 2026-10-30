package net.momirealms.sparrow.data.storage.sql.postgresql;

import net.momirealms.sparrow.data.storage.EntityRef;
import net.momirealms.sparrow.data.storage.EntityStorage;
import net.momirealms.sparrow.data.storage.RankRow;
import net.momirealms.sparrow.data.storage.SaveBatch;
import net.momirealms.sparrow.data.storage.StorageException;
import net.momirealms.sparrow.data.storage.StorageKey;
import net.momirealms.sparrow.data.storage.StoredEntity;
import net.momirealms.sparrow.data.storage.StoredValue;
import net.momirealms.sparrow.data.storage.sql.PathCodec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

public final class PostgresEntityStorage implements EntityStorage {
    private static final int MAX_PREFIX_LENGTH = 24;
    private static final int PURGE_BATCH_SIZE = 1000;

    private final DataSource dataSource;
    private final String tablePrefix;
    private final Map<String, PostgresTables> tablesByType = new ConcurrentHashMap<>();

    // EntityStorage 的 PostgreSQL 实现. 只用 JDK 的 java.sql, 连接池由宿主创建和关闭
    public PostgresEntityStorage(@NotNull DataSource dataSource, @NotNull String tablePrefix) {
        if (tablePrefix.length() > MAX_PREFIX_LENGTH) throw new IllegalArgumentException("Table prefix is longer than " + MAX_PREFIX_LENGTH + " characters: " + tablePrefix);
        if (!tablePrefix.matches("[a-z0-9_]*")) throw new IllegalArgumentException("Table prefix may only contain [a-z0-9_]: " + tablePrefix);
        this.dataSource = dataSource;
        this.tablePrefix = tablePrefix;
    }

    @Override
    public void prepare(@NotNull String entityType) {
        PostgresTables tables = PostgresTables.of(this.tablePrefix + entityType);
        this.withConnection("prepare tables for " + entityType, connection -> {
            try (Statement statement = connection.createStatement()) {
                for (String sql : tables.createStatements()) {
                    statement.execute(sql);
                }
            }
            return null;
        });
        this.tablesByType.put(entityType, tables);
    }

    // 一次取出这个实体的全部行, 在内存里按键 ID 过滤. sortable 的普通键只在排行表里, 其他键都在数据表里
    @NotNull
    @Override
    public StoredEntity load(@NotNull EntityRef entity, @NotNull Collection<StorageKey> keys) {
        PostgresTables tables = this.tables(entity.entityType());
        Set<String> dataKeys = new HashSet<>();
        Set<String> rankedKeys = new HashSet<>();
        for (StorageKey key : keys) {
            if (key.sortable()) {
                rankedKeys.add(key.id());
            } else {
                dataKeys.add(key.id());
            }
        }
        return this.withConnection("load " + entity.entityType(), connection -> {
            Map<String, StoredValue> values = new HashMap<>();
            Map<String, Map<String, StoredValue>> entries = new HashMap<>();
            try (PreparedStatement statement = connection.prepareStatement(tables.selectRows())) {
                statement.setBytes(1, entity.entityId());
                try (ResultSet rows = statement.executeQuery()) {
                    while (rows.next()) {
                        String path = rows.getString(1);
                        String keyId = PathCodec.keyId(path);
                        if (!dataKeys.contains(keyId)) continue;
                        StoredValue value = readValue(rows, 2, 3);
                        String entry = PathCodec.entry(path);
                        if (entry == null) {
                            values.put(keyId, value);
                        } else {
                            entries.computeIfAbsent(keyId, id -> new HashMap<>()).put(entry, value);
                        }
                    }
                }
            }
            if (rankedKeys.isEmpty()) return new StoredEntity(values, entries);
            try (PreparedStatement statement = connection.prepareStatement(tables.selectRankedRows())) {
                statement.setBytes(1, entity.entityId());
                try (ResultSet rows = statement.executeQuery()) {
                    while (rows.next()) {
                        String keyId = rows.getString(1);
                        if (rankedKeys.contains(keyId)) {
                            values.put(keyId, new StoredValue.OfLong(rows.getLong(2)));
                        }
                    }
                }
            }
            return new StoredEntity(values, entries);
        });
    }

    // 一次落库在同一个事务里完成. batch 里每个键和条目最多出现一次, 所以写入和删除可以各自批量执行
    @Override
    public void save(@NotNull EntityRef entity, @NotNull SaveBatch batch) {
        PostgresTables tables = this.tables(entity.entityType());
        this.inTransaction("save " + entity.entityType(), connection -> {
            try (PreparedStatement upsertData = connection.prepareStatement(tables.upsertData());
                 PreparedStatement upsertRanked = connection.prepareStatement(tables.upsertRanked());
                 PreparedStatement deleteData = connection.prepareStatement(tables.deleteRow());
                 PreparedStatement deleteRanked = connection.prepareStatement(tables.deleteRanked())) {
                List<SaveBatch.Write> writes = batch.writes();
                int writeCount = writes.size();
                for (int i = 0; i < writeCount; i++) {
                    SaveBatch.Write write = writes.get(i);
                    if (isRanked(write.key(), write.entry())) {
                        bindRanked(upsertRanked, entity, write.key(), write.value());
                        upsertRanked.addBatch();
                    } else {
                        bindData(upsertData, entity, PathCodec.path(write.key().id(), write.entry()), write.value());
                        upsertData.addBatch();
                    }
                }
                List<SaveBatch.Remove> removes = batch.removes();
                int removeCount = removes.size();
                for (int i = 0; i < removeCount; i++) {
                    SaveBatch.Remove remove = removes.get(i);
                    if (isRanked(remove.key(), remove.entry())) {
                        bindRankedKey(deleteRanked, entity, remove.key());
                        deleteRanked.addBatch();
                    } else {
                        bindPath(deleteData, entity, PathCodec.path(remove.key().id(), remove.entry()));
                        deleteData.addBatch();
                    }
                }
                upsertData.executeBatch();
                upsertRanked.executeBatch();
                deleteData.executeBatch();
                deleteRanked.executeBatch();
            }
            return null;
        });
    }

    @Nullable
    @Override
    public StoredValue read(@NotNull EntityRef entity, @NotNull StorageKey key, @Nullable String entry) {
        PostgresTables tables = this.tables(entity.entityType());
        return this.withConnection("read " + key.id(), connection -> {
            if (isRanked(key, entry)) {
                try (PreparedStatement statement = connection.prepareStatement(tables.selectRanked())) {
                    bindRankedKey(statement, entity, key);
                    try (ResultSet rows = statement.executeQuery()) {
                        return rows.next() ? new StoredValue.OfLong(rows.getLong(1)) : null;
                    }
                }
            }
            try (PreparedStatement statement = connection.prepareStatement(tables.selectRow())) {
                bindPath(statement, entity, PathCodec.path(key.id(), entry));
                try (ResultSet rows = statement.executeQuery()) {
                    return rows.next() ? readValue(rows, 1, 2) : null;
                }
            }
        });
    }

    @NotNull
    @Override
    public Map<String, StoredValue> readEntries(@NotNull EntityRef entity, @NotNull StorageKey key) {
        PostgresTables tables = this.tables(entity.entityType());
        return this.withConnection("read entries of " + key.id(), connection -> {
            Map<String, StoredValue> entries = new HashMap<>();
            try (PreparedStatement statement = connection.prepareStatement(tables.selectEntries())) {
                statement.setBytes(1, entity.entityId());
                statement.setString(2, PathCodec.entriesFrom(key.id()));
                statement.setString(3, PathCodec.entriesTo(key.id()));
                try (ResultSet rows = statement.executeQuery()) {
                    while (rows.next()) {
                        entries.put(PathCodec.entry(rows.getString(1)), readValue(rows, 2, 3));
                    }
                }
            }
            return entries;
        });
    }

    @Override
    public void set(@NotNull EntityRef entity, @NotNull StorageKey key, @Nullable String entry, @NotNull StoredValue value) {
        PostgresTables tables = this.tables(entity.entityType());
        this.withConnection("set " + key.id(), connection -> {
            if (isRanked(key, entry)) {
                try (PreparedStatement statement = connection.prepareStatement(tables.upsertRanked())) {
                    bindRanked(statement, entity, key, value);
                    statement.executeUpdate();
                }
            } else {
                try (PreparedStatement statement = connection.prepareStatement(tables.upsertData())) {
                    bindData(statement, entity, PathCodec.path(key.id(), entry), value);
                    statement.executeUpdate();
                }
            }
            return null;
        });
    }

    @Override
    public boolean compareAndSet(@NotNull EntityRef entity, @NotNull StorageKey key, @Nullable String entry, byte @Nullable [] expected, byte @NotNull [] value) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public boolean remove(@NotNull EntityRef entity, @NotNull StorageKey key, @Nullable String entry) {
        PostgresTables tables = this.tables(entity.entityType());
        return this.withConnection("remove " + key.id(), connection -> {
            if (isRanked(key, entry)) {
                try (PreparedStatement statement = connection.prepareStatement(tables.deleteRanked())) {
                    bindRankedKey(statement, entity, key);
                    return statement.executeUpdate() > 0;
                }
            }
            try (PreparedStatement statement = connection.prepareStatement(tables.deleteRow())) {
                bindPath(statement, entity, PathCodec.path(key.id(), entry));
                return statement.executeUpdate() > 0;
            }
        });
    }

    @Override
    public long add(@NotNull EntityRef entity, @NotNull StorageKey key, @Nullable String entry, long delta) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @NotNull
    @Override
    public OptionalLong take(@NotNull EntityRef entity, @NotNull StorageKey key, @Nullable String entry, long amount) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @NotNull
    @Override
    public List<RankRow> top(@NotNull String entityType, @NotNull StorageKey key, int offset, int limit) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public long rankOf(@NotNull EntityRef entity, @NotNull StorageKey key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void deleteEntity(@NotNull EntityRef entity) {
        PostgresTables tables = this.tables(entity.entityType());
        this.inTransaction("delete " + entity.entityType(), connection -> {
            try (PreparedStatement deleteData = connection.prepareStatement(tables.deleteRows());
                 PreparedStatement deleteRanked = connection.prepareStatement(tables.deleteRankedRows())) {
                deleteData.setBytes(1, entity.entityId());
                deleteData.executeUpdate();
                deleteRanked.setBytes(1, entity.entityId());
                deleteRanked.executeUpdate();
            }
            return null;
        });
    }

    // 跨所有实体删除一个键, 没有索引可用, 要扫全表. 分批删除, 每批单独提交, 每次只锁住一批行
    @Override
    public void purge(@NotNull String entityType, @NotNull StorageKey key) {
        PostgresTables tables = this.tables(entityType);
        this.withConnection("purge " + key.id(), connection -> {
            deleteInBatches(connection, tables.purgeDataBatch(PURGE_BATCH_SIZE), key.id(), PathCodec.entriesFrom(key.id()), PathCodec.entriesTo(key.id()));
            deleteInBatches(connection, tables.purgeRankedBatch(PURGE_BATCH_SIZE), key.id());
            return null;
        });
    }

    @Override
    public boolean transaction(@NotNull Predicate<EntityStorage> body) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @NotNull
    private PostgresTables tables(@NotNull String entityType) {
        return this.tablesByType.get(entityType);
    }

    private <T> T withConnection(@NotNull String action, @NotNull SqlFunction<T> body) {
        try (Connection connection = this.dataSource.getConnection()) {
            return body.apply(connection);
        } catch (SQLException exception) {
            throw new StorageException("Failed to " + action, exception);
        }
    }

    // 关闭自动提交执行 body, 成功就提交, 出错就回滚. 还给连接池之前恢复自动提交, 连接的状态和借出时一样
    private <T> T inTransaction(@NotNull String action, @NotNull SqlFunction<T> body) {
        return this.withConnection(action, connection -> {
            connection.setAutoCommit(false);
            try {
                T result = body.apply(connection);
                connection.commit();
                return result;
            } catch (SQLException | RuntimeException exception) {
                connection.rollback();
                throw exception;
            } finally {
                connection.setAutoCommit(true);
            }
        });
    }

    // 普通键声明了 sortable 时, 值在排行表里; 其他情况都在数据表里
    private static boolean isRanked(@NotNull StorageKey key, @Nullable String entry) {
        return key.sortable() && entry == null;
    }

    // 数据表的一行只有 num 或 val 其中一个有值
    @NotNull
    private static StoredValue readValue(@NotNull ResultSet rows, int numColumn, int valColumn) throws SQLException {
        long number = rows.getLong(numColumn);
        if (!rows.wasNull()) return new StoredValue.OfLong(number);
        return new StoredValue.OfBytes(rows.getBytes(valColumn));
    }

    private static void bindData(@NotNull PreparedStatement statement, @NotNull EntityRef entity, @NotNull String path, @NotNull StoredValue value) throws SQLException {
        bindPath(statement, entity, path);
        switch (value) {
            case StoredValue.OfLong number -> {
                statement.setLong(3, number.value());
                statement.setNull(4, Types.VARBINARY);
            }
            case StoredValue.OfBytes bytes -> {
                statement.setNull(3, Types.BIGINT);
                statement.setBytes(4, bytes.value());
            }
        }
    }

    private static void bindPath(@NotNull PreparedStatement statement, @NotNull EntityRef entity, @NotNull String path) throws SQLException {
        statement.setBytes(1, entity.entityId());
        statement.setString(2, path);
    }

    // 排行表的列顺序是 (data_key, entity_id, num)
    private static void bindRanked(@NotNull PreparedStatement statement, @NotNull EntityRef entity, @NotNull StorageKey key, @NotNull StoredValue value) throws SQLException {
        statement.setString(1, key.id());
        statement.setBytes(2, entity.entityId());
        statement.setLong(3, ((StoredValue.OfLong) value).value());
    }

    private static void bindRankedKey(@NotNull PreparedStatement statement, @NotNull EntityRef entity, @NotNull StorageKey key) throws SQLException {
        statement.setBytes(1, entity.entityId());
        statement.setString(2, key.id());
    }

    private static void deleteInBatches(@NotNull Connection connection, @NotNull String sql, @NotNull String... parameters) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int i = 0; i < parameters.length; i++) {
                statement.setString(i + 1, parameters[i]);
            }
            int deleted;
            do {
                deleted = statement.executeUpdate();
            } while (deleted == PURGE_BATCH_SIZE);
        }
    }

    @FunctionalInterface
    private interface SqlFunction<T> {

        T apply(@NotNull Connection connection) throws SQLException;
    }
}