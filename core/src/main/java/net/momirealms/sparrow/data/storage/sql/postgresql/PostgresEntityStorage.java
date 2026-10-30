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

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;

public final class PostgresEntityStorage implements EntityStorage {
    private static final int MAX_PREFIX_LENGTH = 24;
    private static final int TRANSACTION_ATTEMPTS = 3;

    private final DataSource dataSource;
    private final String tablePrefix;
    private final Map<String, PostgresTables> tablesByType = new ConcurrentHashMap<>();

    // EntityStorage 的 PostgreSQL 实现. 只用 JDK 的 java.sql, 连接池由宿主创建和关闭.
    // 这个类负责借连接和划定事务, SQL 都在 PostgresConnectionStorage 里
    public PostgresEntityStorage(@NotNull DataSource dataSource, @NotNull String tablePrefix) {
        if (tablePrefix.length() > MAX_PREFIX_LENGTH) throw new IllegalArgumentException("Table prefix is longer than " + MAX_PREFIX_LENGTH + " characters: " + tablePrefix);
        if (!tablePrefix.matches("[a-z0-9_]*")) throw new IllegalArgumentException("Table prefix may only contain [a-z0-9_]: " + tablePrefix);
        this.dataSource = dataSource;
        this.tablePrefix = tablePrefix;
    }

    @Override
    public void prepare(@NotNull String entityType) {
        this.withConnection(storage -> {
            storage.prepare(entityType);
            return null;
        });
    }

    @NotNull
    @Override
    public StoredEntity load(@NotNull EntityRef entity, @NotNull Collection<StorageKey> keys) {
        return this.withConnection(storage -> storage.load(entity, keys));
    }

    @Override
    public void save(@NotNull EntityRef entity, @NotNull SaveBatch batch) {
        this.inTransaction(storage -> {
            storage.save(entity, batch);
            return null;
        });
    }

    @Nullable
    @Override
    public StoredValue read(@NotNull EntityRef entity, @NotNull StorageKey key, @Nullable String entry) {
        return this.withConnection(storage -> storage.read(entity, key, entry));
    }

    @NotNull
    @Override
    public Map<String, StoredValue> readEntries(@NotNull EntityRef entity, @NotNull StorageKey key) {
        return this.withConnection(storage -> storage.readEntries(entity, key));
    }

    @Override
    public void set(@NotNull EntityRef entity, @NotNull StorageKey key, @Nullable String entry, @NotNull StoredValue value) {
        this.withConnection(storage -> {
            storage.set(entity, key, entry, value);
            return null;
        });
    }

    @Override
    public boolean compareAndSet(@NotNull EntityRef entity, @NotNull StorageKey key, @Nullable String entry, byte @Nullable [] expected, byte @NotNull [] value) {
        return this.withConnection(storage -> storage.compareAndSet(entity, key, entry, expected, value));
    }

    @Override
    public boolean remove(@NotNull EntityRef entity, @NotNull StorageKey key, @Nullable String entry) {
        return this.withConnection(storage -> storage.remove(entity, key, entry));
    }

    @Override
    public long add(@NotNull EntityRef entity, @NotNull StorageKey key, @Nullable String entry, long delta) {
        return this.withConnection(storage -> storage.add(entity, key, entry, delta));
    }

    @NotNull
    @Override
    public OptionalLong take(@NotNull EntityRef entity, @NotNull StorageKey key, @Nullable String entry, long amount) {
        return this.withConnection(storage -> storage.take(entity, key, entry, amount));
    }

    @NotNull
    @Override
    public List<RankRow> top(@NotNull String entityType, @NotNull StorageKey key, int offset, int limit) {
        return this.withConnection(storage -> storage.top(entityType, key, offset, limit));
    }

    @Override
    public long countAbove(@NotNull String entityType, @NotNull StorageKey key, long value) {
        return this.withConnection(storage -> storage.countAbove(entityType, key, value));
    }

    @Override
    public void deleteEntity(@NotNull EntityRef entity) {
        this.inTransaction(storage -> {
            storage.deleteEntity(entity);
            return null;
        });
    }

    @Override
    public void purge(@NotNull String entityType, @NotNull StorageKey key) {
        this.withConnection(storage -> {
            storage.purge(entityType, key);
            return null;
        });
    }

    // 死锁或串行化失败时数据库让其中一个事务失败, 这里回滚后重跑整个 body, 最多跑 3 次, 所以 body 可能执行多次.
    // body 返回 false 时先在这里回滚, inTransaction 随后的提交就没有内容可提交
    @Override
    public boolean transaction(@NotNull Predicate<EntityStorage> body) {
        for (int attempt = 1; ; attempt++) {
            try {
                return this.inTransaction(storage -> {
                    if (body.test(storage)) return true;
                    storage.rollback();
                    return false;
                });
            } catch (StorageException exception) {
                if (attempt == TRANSACTION_ATTEMPTS || !isConcurrencyFailure(exception)) throw exception;
            }
        }
    }

    private <T> T withConnection(@NotNull Function<PostgresConnectionStorage, T> body) {
        try (Connection connection = this.dataSource.getConnection()) {
            return body.apply(new PostgresConnectionStorage(connection, this.tablePrefix, this.tablesByType, false));
        } catch (SQLException exception) {
            throw new StorageException("Failed to get or release a connection", exception);
        }
    }

    // 关闭自动提交执行 body, 正常返回就提交, 抛异常就回滚. 还给连接池之前恢复自动提交, 连接的状态和借出时一样
    private <T> T inTransaction(@NotNull Function<PostgresConnectionStorage, T> body) {
        try (Connection connection = this.dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                T result = body.apply(new PostgresConnectionStorage(connection, this.tablePrefix, this.tablesByType, true));
                connection.commit();
                return result;
            } catch (SQLException | RuntimeException exception) {
                connection.rollback();
                throw exception;
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (SQLException exception) {
            throw new StorageException("Failed to run a transaction", exception);
        }
    }

    // 40P01 是死锁; 40001 是隔离级别为 REPEATABLE READ 或 SERIALIZABLE 时的串行化失败, 同样是并发冲突, 重跑就能继续
    private static boolean isConcurrencyFailure(@NotNull StorageException exception) {
        if (!(exception.getCause() instanceof SQLException cause)) return false;
        String state = cause.getSQLState();
        return "40P01".equals(state) || "40001".equals(state);
    }
}