package net.momirealms.sparrow.data.storage.sql.mysql;

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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Predicate;

final class MysqlConnectionStorage implements EntityStorage {
    private static final int PURGE_BATCH_SIZE = 1000;

    private final Connection connection;
    private final String tablePrefix;
    private final Map<String, MysqlTables> tablesByType;
    private final boolean lockReads;

    // 一个连接上的 EntityStorage, 每次借连接时创建, 还连接时丢弃. 事务由 MysqlEntityStorage 划定.
    // 在事务里时 lockReads 为 true, 读取加 FOR UPDATE, 读到的是最新提交的值, 并且这一行锁到事务结束
    MysqlConnectionStorage(@NotNull Connection connection, @NotNull String tablePrefix, @NotNull Map<String, MysqlTables> tablesByType, boolean lockReads) {
        this.connection = connection;
        this.tablePrefix = tablePrefix;
        this.tablesByType = tablesByType;
        this.lockReads = lockReads;
    }

    @Override
    public void prepare(@NotNull String entityType) {
        MysqlTables tables = MysqlTables.of(this.tablePrefix + entityType);
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
        MysqlTables tables = this.tables(entity.entityType());
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
            Map<String, Map<String, StoredValue>> entries = new HashMap<>();
            try (PreparedStatement statement = this.connection.prepareStatement(tables.selectRows())) {
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
            return new StoredEntity(values, entries);
        });
    }

    // 调用方要在事务里调用. batch 里每个键和条目最多出现一次, 所以写入和删除可以各自批量执行
    @Override
    public void save(@NotNull EntityRef entity, @NotNull SaveBatch batch) {
        MysqlTables tables = this.tables(entity.entityType());
        this.execute("save " + entity.entityType(), () -> {
            try (PreparedStatement upsertData = this.connection.prepareStatement(tables.upsertData());
                 PreparedStatement upsertRanked = this.connection.prepareStatement(tables.upsertRanked());
                 PreparedStatement deleteData = this.connection.prepareStatement(tables.deleteRow());
                 PreparedStatement deleteRanked = this.connection.prepareStatement(tables.deleteRanked())) {
                List<SaveBatch.Write> writes = batch.writes();
                int writeCount = writes.size();
                for (int i = 0; i < writeCount; i++) {
                    SaveBatch.Write write = writes.get(i);
                    boolean ranked = isRanked(write.key(), write.entry());
                    PreparedStatement statement = ranked ? upsertRanked : upsertData;
                    bindTarget(statement, 1, entity, write.key(), write.entry());
                    bindValue(statement, ranked, write.value());
                    statement.addBatch();
                }
                List<SaveBatch.Remove> removes = batch.removes();
                int removeCount = removes.size();
                for (int i = 0; i < removeCount; i++) {
                    SaveBatch.Remove remove = removes.get(i);
                    PreparedStatement statement = isRanked(remove.key(), remove.entry()) ? deleteRanked : deleteData;
                    bindTarget(statement, 1, entity, remove.key(), remove.entry());
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
    public StoredValue read(@NotNull EntityRef entity, @NotNull StorageKey key, @Nullable String entry) {
        MysqlTables tables = this.tables(entity.entityType());
        return this.execute("read " + key.id(), () -> {
            if (isRanked(key, entry)) {
                try (PreparedStatement statement = this.connection.prepareStatement(this.lockReads ? tables.lockRanked() : tables.selectRanked())) {
                    bindTarget(statement, 1, entity, key, entry);
                    try (ResultSet rows = statement.executeQuery()) {
                        return rows.next() ? new StoredValue.OfLong(rows.getLong(1)) : null;
                    }
                }
            }
            try (PreparedStatement statement = this.connection.prepareStatement(this.lockReads ? tables.lockRow() : tables.selectRow())) {
                bindTarget(statement, 1, entity, key, entry);
                try (ResultSet rows = statement.executeQuery()) {
                    return rows.next() ? readValue(rows, 1, 2) : null;
                }
            }
        });
    }

    @NotNull
    @Override
    public Map<String, StoredValue> readEntries(@NotNull EntityRef entity, @NotNull StorageKey key) {
        MysqlTables tables = this.tables(entity.entityType());
        return this.execute("read entries of " + key.id(), () -> {
            Map<String, StoredValue> entries = new HashMap<>();
            try (PreparedStatement statement = this.connection.prepareStatement(tables.selectEntries())) {
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
        MysqlTables tables = this.tables(entity.entityType());
        boolean ranked = isRanked(key, entry);
        this.execute("set " + key.id(), () -> {
            try (PreparedStatement statement = this.connection.prepareStatement(ranked ? tables.upsertRanked() : tables.upsertData())) {
                bindTarget(statement, 1, entity, key, entry);
                bindValue(statement, ranked, value);
                statement.executeUpdate();
            }
            return null;
        });
    }

    @Override
    public boolean compareAndSet(@NotNull EntityRef entity, @NotNull StorageKey key, @Nullable String entry, byte @Nullable [] expected, byte @NotNull [] value) {
        // 新旧字节相同时 UPDATE 不会改动这一行, 影响行数取决于驱动的 useAffectedRows 设置. 这种情况只需要确认当前值等于 expected
        if (expected != null && Arrays.equals(expected, value)) {
            return this.read(entity, key, entry) instanceof StoredValue.OfBytes current && Arrays.equals(current.value(), expected);
        }
        MysqlTables tables = this.tables(entity.entityType());
        return this.execute("compare and set " + key.id(), () -> {
            if (expected == null) {
                // 主键冲突说明已经有值. MySQL 只回滚出错的这一条语句, 所在的事务可以继续
                try (PreparedStatement statement = this.connection.prepareStatement(tables.insertData())) {
                    bindTarget(statement, 1, entity, key, entry);
                    statement.setBytes(3, value);
                    statement.executeUpdate();
                    return true;
                } catch (SQLIntegrityConstraintViolationException exception) {
                    return false;
                }
            }
            try (PreparedStatement statement = this.connection.prepareStatement(tables.compareAndSetData())) {
                statement.setBytes(1, value);
                bindTarget(statement, 2, entity, key, entry);
                statement.setBytes(4, expected);
                return statement.executeUpdate() > 0;
            }
        });
    }

    @Override
    public boolean compareAndRemove(@NotNull EntityRef entity, @NotNull StorageKey key, @Nullable String entry, byte @NotNull [] expected) {
        MysqlTables tables = this.tables(entity.entityType());
        return this.execute("compare and remove " + key.id(), () -> {
            try (PreparedStatement statement = this.connection.prepareStatement(tables.compareAndRemoveData())) {
                bindTarget(statement, 1, entity, key, entry);
                statement.setBytes(3, expected);
                return statement.executeUpdate() > 0;
            }
        });
    }

    @Override
    public boolean remove(@NotNull EntityRef entity, @NotNull StorageKey key, @Nullable String entry) {
        MysqlTables tables = this.tables(entity.entityType());
        return this.execute("remove " + key.id(), () -> {
            try (PreparedStatement statement = this.connection.prepareStatement(isRanked(key, entry) ? tables.deleteRanked() : tables.deleteRow())) {
                bindTarget(statement, 1, entity, key, entry);
                return statement.executeUpdate() > 0;
            }
        });
    }

    // 调用方要在事务里调用. MySQL 没有 RETURNING, upsert 之后在同一个事务里加锁读回新值, 这期间这一行一直被锁着
    @Override
    public long add(@NotNull EntityRef entity, @NotNull StorageKey key, @Nullable String entry, long delta) {
        MysqlTables tables = this.tables(entity.entityType());
        boolean ranked = isRanked(key, entry);
        return this.execute("add to " + key.id(), () -> {
            try (PreparedStatement statement = this.connection.prepareStatement(ranked ? tables.addRanked() : tables.addData())) {
                bindTarget(statement, 1, entity, key, entry);
                statement.setLong(3, delta);
                statement.executeUpdate();
            }
            try (PreparedStatement statement = this.connection.prepareStatement(ranked ? tables.lockRanked() : tables.lockRow())) {
                bindTarget(statement, 1, entity, key, entry);
                try (ResultSet rows = statement.executeQuery()) {
                    rows.next();
                    return rows.getLong(1);
                }
            }
        });
    }

    // 调用方要在事务里调用. 先加锁读出余额, 判断够不够再写回, 判断和扣减之间别人改不了这一行. 返回扣减后的余额
    @NotNull
    @Override
    public OptionalLong take(@NotNull EntityRef entity, @NotNull StorageKey key, @Nullable String entry, long amount) {
        MysqlTables tables = this.tables(entity.entityType());
        boolean ranked = isRanked(key, entry);
        return this.execute("take from " + key.id(), () -> {
            long balance;
            try (PreparedStatement statement = this.connection.prepareStatement(ranked ? tables.lockRanked() : tables.lockRow())) {
                bindTarget(statement, 1, entity, key, entry);
                try (ResultSet rows = statement.executeQuery()) {
                    if (!rows.next()) return OptionalLong.empty();
                    balance = rows.getLong(1);
                }
            }
            if (balance < amount) return OptionalLong.empty();
            try (PreparedStatement statement = this.connection.prepareStatement(ranked ? tables.updateRankedNum() : tables.updateDataNum())) {
                statement.setLong(1, balance - amount);
                bindTarget(statement, 2, entity, key, entry);
                statement.executeUpdate();
            }
            return OptionalLong.of(balance - amount);
        });
    }

    @NotNull
    @Override
    public List<RankRow> top(@NotNull String entityType, @NotNull StorageKey key, int offset, int limit) {
        MysqlTables tables = this.tables(entityType);
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
        MysqlTables tables = this.tables(entityType);
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
        MysqlTables tables = this.tables(entity.entityType());
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
        MysqlTables tables = this.tables(entityType);
        this.execute("purge " + keyId, () -> {
            this.deleteInBatches(tables.purgeDataBatch(PURGE_BATCH_SIZE), keyId, PathCodec.entriesFrom(keyId), PathCodec.entriesTo(keyId));
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
    private MysqlTables tables(@NotNull String entityType) {
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

    // 普通键声明了 sortable 时, 值在排行表里; 其他情况都在数据表里
    private static boolean isRanked(@NotNull StorageKey key, @Nullable String entry) {
        return key.sortable() && entry == null;
    }

    // 数据表按 (entity_id, path) 定位一行, 排行表按 (entity_id, data_key) 定位, 两者的参数顺序相同
    private static void bindTarget(@NotNull PreparedStatement statement, int index, @NotNull EntityRef entity, @NotNull StorageKey key, @Nullable String entry) throws SQLException {
        statement.setBytes(index, entity.entityId());
        statement.setString(index + 1, isRanked(key, entry) ? key.id() : PathCodec.path(key.id(), entry));
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