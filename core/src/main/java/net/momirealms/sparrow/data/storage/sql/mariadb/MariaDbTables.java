package net.momirealms.sparrow.data.storage.sql.mariadb;

import org.jetbrains.annotations.NotNull;

import java.util.List;

record MariaDbTables(@NotNull String data, @NotNull String ranked, @NotNull String rankIndex) {

    // 一个实体类型的两张表. 表名和索引名都带上前缀和类型名, 最长 61 个字符
    @NotNull
    static MariaDbTables of(@NotNull String baseName) {
        return new MariaDbTables(baseName + "_data", baseName + "_ranked", baseName + "_rank");
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
    String insertData() {
        return "INSERT IGNORE INTO " + this.data + " (entity_id, path, val) VALUES (?, ?, ?)";
    }

    @NotNull
    String addData() {
        return "INSERT INTO " + this.data + " (entity_id, path, num) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE num = num + VALUES(num) RETURNING num";
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
    String lockRow() {
        return "SELECT num, val FROM " + this.data + " WHERE entity_id = ? AND path = ? FOR UPDATE";
    }

    @NotNull
    String selectEntries() {
        return "SELECT path, num, val FROM " + this.data + " WHERE entity_id = ? AND path >= ? AND path < ?";
    }

    @NotNull
    String compareAndSetData() {
        return "UPDATE " + this.data + " SET val = ? WHERE entity_id = ? AND path = ? AND val = ?";
    }

    @NotNull
    String updateDataNum() {
        return "UPDATE " + this.data + " SET num = ? WHERE entity_id = ? AND path = ?";
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
        return "INSERT INTO " + this.ranked + " (entity_id, data_key, num) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE num = VALUES(num)";
    }

    @NotNull
    String addRanked() {
        return "INSERT INTO " + this.ranked + " (entity_id, data_key, num) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE num = num + VALUES(num) RETURNING num";
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
    String lockRanked() {
        return "SELECT num FROM " + this.ranked + " WHERE entity_id = ? AND data_key = ? FOR UPDATE";
    }

    @NotNull
    String updateRankedNum() {
        return "UPDATE " + this.ranked + " SET num = ? WHERE entity_id = ? AND data_key = ?";
    }

    // 索引是 (data_key, num, entity_id) 升序, 两列都按降序排时可以反向扫描索引, 不需要额外排序
    @NotNull
    String top() {
        return "SELECT entity_id, num FROM " + this.ranked + " WHERE data_key = ? ORDER BY num DESC, entity_id DESC LIMIT ? OFFSET ?";
    }

    @NotNull
    String countAbove() {
        return "SELECT COUNT(*) FROM " + this.ranked + " WHERE data_key = ? AND num > ?";
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