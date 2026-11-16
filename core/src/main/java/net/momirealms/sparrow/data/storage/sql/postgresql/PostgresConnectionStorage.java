package net.momirealms.sparrow.data.storage.sql.postgresql;

import net.momirealms.sparrow.data.storage.EntityRef;
import net.momirealms.sparrow.data.storage.EntityStorage;
import net.momirealms.sparrow.data.storage.RankRow;
import net.momirealms.sparrow.data.storage.SaveBatch;
import net.momirealms.sparrow.data.storage.StorageException;
import net.momirealms.sparrow.data.storage.StorageKey;
import net.momirealms.sparrow.data.storage.StoredEntity;
import net.momirealms.sparrow.data.storage.StoredValue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Predicate;

final class PostgresConnectionStorage implements EntityStorage {
    private static final int PURGE_BATCH_SIZE = 1000;

    private final Connection connection;
    private final String tablePrefix;
    private final Map<String, PostgresTables> tablesByType;
    private final boolean lockReads;

    // 一个连接上的 EntityStorage, 每次借连接时创建, 还连接时丢弃. 事务由 PostgresEntityStorage 划定.
    // 在事务里时 lockReads 为 true, 读取加 FOR UPDATE, 读到的是最新提交的值, 并且这一行锁到事务结束
    PostgresConnectionStorage(@NotNull Connection connection, @NotNull String tablePrefix, @NotNull Map<String, PostgresTables> tablesByType, boolean lockReads) {
        this.connection = connection;
        this.tablePrefix = tablePrefix;
        this.tablesByType = tablesByType;
        this.lockReads = lockReads;
    }

    @Override
    public void prepare(@NotNull String entityType) {
        PostgresTables tables = PostgresTables.of(this.tablePrefix + entityType);
        this.execute("prepare tables for " + entityType, () -> {
            try (Statement statement = this.connection.createStatement()) {
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
        return this.execute("load " + entity.entityType(), () -> {
            Map<String, StoredValue> values = new HashMap<>();
            try (PreparedStatement statement = this.connection.prepareStatement(tables.selectRows())) {
                statement.setBytes(1, entity.entityId());
                try (ResultSet rows = statement.executeQuery()) {
                    while (rows.next()) {
                        String keyId = rows.getString(1);
                        if (dataKeys.contains(keyId)) {
                            values.put(keyId, readValue(rows, 2, 3));
                        }
                    }
                }
            }
            if (rankedKeys.isEmpty()) return new StoredEntity(values);
            try (PreparedStatement statement = this.connection.prepareStatement(tables.selectRankedRows())) {
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
            return new StoredEntity(values);
        });
    }

    // 调用方要在事务里调用. batch 里每个键最多出现一次, 所以写入和删除可以各自批量执行
    @Override
    public void save(@NotNull EntityRef entity, @NotNull SaveBatch batch) {
        PostgresTables tables = this.tables(entity.entityType());
        this.execute("save " + entity.entityType(), () -> {
            try (PreparedStatement upsertData = this.connection.prepareStatement(tables.upsertData());
                 PreparedStatement upsertRanked = this.connection.prepareStatement(tables.upsertRanked());
                 PreparedStatement deleteData = this.connection.prepareStatement(tables.deleteRow());
                 PreparedStatement deleteRanked = this.connection.prepareStatement(tables.deleteRanked())) {
                List<SaveBatch.Write> writes = batch.writes();
                int writeCount = writes.size();
                for (int i = 0; i < writeCount; i++) {
                    SaveBatch.Write write = writes.get(i);
                    boolean ranked = write.key().sortable();
                    PreparedStatement statement = ranked ? upsertRanked : upsertData;
                    bindTarget(statement, 1, entity, write.key());
                    bindValue(statement, ranked, write.value());
                    statement.addBatch();
                }
                List<SaveBatch.Remove> removes = batch.removes();
                int removeCount = removes.size();
                for (int i = 0; i < removeCount; i++) {
                    SaveBatch.Remove remove = removes.get(i);
                    PreparedStatement statement = remove.key().sortable() ? deleteRanked : deleteData;
                    bindTarget(statement, 1, entity, remove.key());
                    statement.addBatch();
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
    public StoredValue read(@NotNull EntityRef entity, @NotNull StorageKey key) {
        PostgresTables tables = this.tables(entity.entityType());
        return this.execute("read " + key.id(), () -> {
            if (key.sortable()) {
                try (PreparedStatement statement = this.connection.prepareStatement(this.lockReads ? tables.lockRanked() : tables.selectRanked())) {
                    bindTarget(statement, 1, entity, key);
                    try (ResultSet rows = statement.executeQuery()) {
                        return rows.next() ? new StoredValue.OfLong(rows.getLong(1)) : null;
                    }
                }
            }
            try (PreparedStatement statement = this.connection.prepareStatement(this.lockReads ? tables.lockRow() : tables.selectRow())) {
                bindTarget(statement, 1, entity, key);
                try (ResultSet rows = statement.executeQuery()) {
                    return rows.next() ? readValue(rows, 1, 2) : null;
                }
            }
        });
    }

    @Override
    public void set(@NotNull EntityRef entity, @NotNull StorageKey key, @NotNull StoredValue value) {
        PostgresTables tables = this.tables(entity.entityType());
        boolean ranked = key.sortable();
        this.execute("set " + key.id(), () -> {
            try (PreparedStatement statement = this.connection.prepareStatement(ranked ? tables.upsertRanked() : tables.upsertData())) {
                bindTarget(statement, 1, entity, key);
                bindValue(statement, ranked, value);
                statement.executeUpdate();
            }
            return null;
        });
    }

    // PostgreSQL 的影响行数是匹配到的行数, 新旧字节相同时也算一行
    @Override
    public boolean compareAndSet(@NotNull EntityRef entity, @NotNull StorageKey key, byte @Nullable [] expected, byte @NotNull [] value) {
        PostgresTables tables = this.tables(entity.entityType());
        return this.execute("compare and set " + key.id(), () -> {
            if (expected == null) {
                // 主键冲突时 DO NOTHING 插入 0 行. 不抛异常, 所在的事务不会因此中止
                try (PreparedStatement statement = this.connection.prepareStatement(tables.insertData())) {
                    bindTarget(statement, 1, entity, key);
                    statement.setBytes(3, value);
                    return statement.executeUpdate() > 0;
                }
            }
            try (PreparedStatement statement = this.connection.prepareStatement(tables.compareAndSetData())) {
                statement.setBytes(1, value);
                bindTarget(statement, 2, entity, key);
                statement.setBytes(4, expected);
                return statement.executeUpdate() > 0;
            }
        });
    }

    @Override
    public boolean compareAndRemove(@NotNull EntityRef entity, @NotNull StorageKey key, byte @NotNull [] expected) {
        PostgresTables tables = this.tables(entity.entityType());
        return this.execute("compare and remove " + key.id(), () -> {
            try (PreparedStatement statement = this.connection.prepareStatement(tables.compareAndRemoveData())) {
                bindTarget(statement, 1, entity, key);
                statement.setBytes(3, expected);
                return statement.executeUpdate() > 0;
            }
        });
    }

    @Override
    public boolean remove(@NotNull EntityRef entity, @NotNull StorageKey key) {
        PostgresTables tables = this.tables(entity.entityType());
        return this.execute("remove " + key.id(), () -> {
            try (PreparedStatement statement = this.connection.prepareStatement(key.sortable() ? tables.deleteRanked() : tables.deleteRow())) {
                bindTarget(statement, 1, entity, key);
                return statement.executeUpdate() > 0;
            }
        });
    }

    // upsert 带 RETURNING, 一条语句就拿到新值
    @Override
    public long add(@NotNull EntityRef entity, @NotNull StorageKey key, long delta) {
        PostgresTables tables = this.tables(entity.entityType());
        return this.execute("add to " + key.id(), () -> {
            try (PreparedStatement statement = this.connection.prepareStatement(key.sortable() ? tables.addRanked() : tables.addData())) {
                bindTarget(statement, 1, entity, key);
                statement.setLong(3, delta);
                try (ResultSet rows = statement.executeQuery()) {
                    rows.next();
                    return rows.getLong(1);
                }
            }
        });
    }

    // 余额条件写在 UPDATE 里, 判断和扣减在同一条语句里完成. 没有返回行就是余额不足或者行不存在. 返回扣减后的余额
    @NotNull
    @Override
    public OptionalLong take(@NotNull EntityRef entity, @NotNull StorageKey key, long amount) {
        PostgresTables tables = this.tables(entity.entityType());
        return this.execute("take from " + key.id(), () -> {
            try (PreparedStatement statement = this.connection.prepareStatement(key.sortable() ? tables.takeRanked() : tables.takeData())) {
                statement.setLong(1, amount);
                bindTarget(statement, 2, entity, key);
                statement.setLong(4, amount);
                try (ResultSet rows = statement.executeQuery()) {
                    return rows.next() ? OptionalLong.of(rows.getLong(1)) : OptionalLong.empty();
                }
            }
        });
    }

    @NotNull
    @Override
    public List<RankRow> top(@NotNull String entityType, @NotNull StorageKey key, int offset, int limit) {
        PostgresTables tables = this.tables(entityType);
        return this.execute("read top of " + key.id(), () -> {
            List<RankRow> top = new ArrayList<>();
            try (PreparedStatement statement = this.connection.prepareStatement(tables.top())) {
                statement.setString(1, key.id());
                statement.setInt(2, limit);
                statement.setInt(3, offset);
                try (ResultSet rows = statement.executeQuery()) {
                    while (rows.next()) {
                        top.add(new RankRow(rows.getBytes(1), rows.getLong(2)));
                    }
                }
            }
            return top;
        });
    }

    @Override
    public long countAbove(@NotNull String entityType, @NotNull StorageKey key, long value) {
        PostgresTables tables = this.tables(entityType);
        return this.execute("count above in " + key.id(), () -> {
            try (PreparedStatement statement = this.connection.prepareStatement(tables.countAbove())) {
                statement.setString(1, key.id());
                statement.setLong(2, value);
                try (ResultSet rows = statement.executeQuery()) {
                    rows.next();
                    return rows.getLong(1);
                }
            }
        });
    }

    // 调用方要在事务里调用
    @Override
    public void deleteEntity(@NotNull EntityRef entity) {
        PostgresTables tables = this.tables(entity.entityType());
        this.execute("delete " + entity.entityType(), () -> {
            try (PreparedStatement deleteData = this.connection.prepareStatement(tables.deleteRows());
                 PreparedStatement deleteRanked = this.connection.prepareStatement(tables.deleteRankedRows())) {
                deleteData.setBytes(1, entity.entityId());
                deleteData.executeUpdate();
                deleteRanked.setBytes(1, entity.entityId());
                deleteRanked.executeUpdate();
            }
            return null;
        });
    }

    // 跨所有实体删除一个键, 没有索引可用, 要扫全表. 在自动提交的连接上分批删除, 每批单独提交, 每次只锁住一批行
    @Override
    public void purge(@NotNull String entityType, @NotNull String keyId) {
        PostgresTables tables = this.tables(entityType);
        this.execute("purge " + keyId, () -> {
            this.deleteInBatches(tables.purgeDataBatch(PURGE_BATCH_SIZE), keyId);
            this.deleteInBatches(tables.purgeRankedBatch(PURGE_BATCH_SIZE), keyId);
            return null;
        });
    }

    @Override
    public boolean transaction(@NotNull Predicate<EntityStorage> body) {
        throw new UnsupportedOperationException("Nested transactions are not supported");
    }

    void rollback() {
        this.execute("roll back", () -> {
            this.connection.rollback();
            return null;
        });
    }

    @NotNull
    private PostgresTables tables(@NotNull String entityType) {
        return this.tablesByType.get(entityType);
    }

    private <T> T execute(@NotNull String action, @NotNull SqlSupplier<T> body) {
        try {
            return body.get();
        } catch (SQLException exception) {
            throw new StorageException("Failed to " + action, exception);
        }
    }

    private void deleteInBatches(@NotNull String sql, @NotNull String... parameters) throws SQLException {
        try (PreparedStatement statement = this.connection.prepareStatement(sql)) {
            for (int i = 0; i < parameters.length; i++) {
                statement.setString(i + 1, parameters[i]);
            }
            int deleted;
            do {
                deleted = statement.executeUpdate();
            } while (deleted == PURGE_BATCH_SIZE);
        }
    }

    // 数据表按 (entity_id, path) 定位一行, 排行表按 (entity_id, data_key) 定位, 两者的参数顺序相同
    private static void bindTarget(@NotNull PreparedStatement statement, int index, @NotNull EntityRef entity, @NotNull StorageKey key) throws SQLException {
        statement.setBytes(index, entity.entityId());
        statement.setString(index + 1, key.id());
    }

    // 值从第 3 个参数开始. 排行表只有 num 列; 数据表的一行只有 num 或 val 其中一个有值
    private static void bindValue(@NotNull PreparedStatement statement, boolean ranked, @NotNull StoredValue value) throws SQLException {
        if (ranked) {
            statement.setLong(3, ((StoredValue.OfLong) value).value());
            return;
        }
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

    @NotNull
    private static StoredValue readValue(@NotNull ResultSet rows, int numColumn, int valColumn) throws SQLException {
        long number = rows.getLong(numColumn);
        if (!rows.wasNull()) return new StoredValue.OfLong(number);
        return new StoredValue.OfBytes(rows.getBytes(valColumn));
    }

    @FunctionalInterface
    private interface SqlSupplier<T> {

        T get() throws SQLException;
    }
}