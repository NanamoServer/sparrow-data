package net.momirealms.sparrow.data.storage.sql.mysql;

import org.jetbrains.annotations.NotNull;

import java.util.List;

record MysqlTables(@NotNull String data, @NotNull String ranked, @NotNull String rankIndex) {

    // 一个实体类型的两张表. 表名和索引名都带上前缀和类型名, 最长 61 个字符
    @NotNull
    static MysqlTables of(@NotNull String baseName) {
        return new MysqlTables(baseName + "_data", baseName + "_ranked", baseName + "_rank");
    }

    // path 和 data_key 按字节比较, PathCodec 的条目范围查询依赖这个排序
    @NotNull
    List<String> createStatements() {
        return List.of(
                """
                CREATE TABLE IF NOT EXISTS %s (
                    entity_id VARBINARY(64) NOT NULL,
                    path VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                    num BIGINT NULL,
                    val MEDIUMBLOB NULL,
                    PRIMARY KEY (entity_id, path)
                ) ENGINE = InnoDB""".formatted(this.data),
                """
                CREATE TABLE IF NOT EXISTS %s (
                    data_key VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                    entity_id VARBINARY(64) NOT NULL,
                    num BIGINT NOT NULL,
                    PRIMARY KEY (entity_id, data_key),
                    INDEX %s (data_key, num, entity_id)
                ) ENGINE = InnoDB""".formatted(this.ranked, this.rankIndex)
        );
    }

    @NotNull
    String upsertData() {
        return "INSERT INTO " + this.data + " (entity_id, path, num, val) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE num = VALUES(num), val = VALUES(val)";
    }

    @NotNull
    String selectRows() {
        return "SELECT path, num, val FROM " + this.data + " WHERE entity_id = ?";
    }

    @NotNull
    String selectRow() {
        return "SELECT num, val FROM " + this.data + " WHERE entity_id = ? AND path = ?";
    }

    @NotNull
    String selectEntries() {
        return "SELECT path, num, val FROM " + this.data + " WHERE entity_id = ? AND path >= ? AND path < ?";
    }

    @NotNull
    String deleteRow() {
        return "DELETE FROM " + this.data + " WHERE entity_id = ? AND path = ?";
    }

    @NotNull
    String deleteRows() {
        return "DELETE FROM " + this.data + " WHERE entity_id = ?";
    }

    @NotNull
    String purgeDataBatch(int limit) {
        return "DELETE FROM " + this.data + " WHERE path = ? OR (path >= ? AND path < ?) LIMIT " + limit;
    }

    @NotNull
    String upsertRanked() {
        return "INSERT INTO " + this.ranked + " (data_key, entity_id, num) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE num = VALUES(num)";
    }

    @NotNull
    String selectRankedRows() {
        return "SELECT data_key, num FROM " + this.ranked + " WHERE entity_id = ?";
    }

    @NotNull
    String selectRanked() {
        return "SELECT num FROM " + this.ranked + " WHERE entity_id = ? AND data_key = ?";
    }

    @NotNull
    String deleteRanked() {
        return "DELETE FROM " + this.ranked + " WHERE entity_id = ? AND data_key = ?";
    }

    @NotNull
    String deleteRankedRows() {
        return "DELETE FROM " + this.ranked + " WHERE entity_id = ?";
    }

    @NotNull
    String purgeRankedBatch(int limit) {
        return "DELETE FROM " + this.ranked + " WHERE data_key = ? LIMIT " + limit;
    }
}