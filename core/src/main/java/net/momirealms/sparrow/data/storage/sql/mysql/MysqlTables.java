package net.momirealms.sparrow.data.storage.sql.mysql;

import org.jetbrains.annotations.NotNull;

import java.util.List;

record MysqlTables(@NotNull String data, @NotNull String ranked, @NotNull String rankIndex, @NotNull String rankDoubleIndex) {

    // 一个实体类型的两张表. 表名和索引名都带上前缀和类型名, 最长 62 个字符
    @NotNull
    static MysqlTables of(@NotNull String baseName) {
        return new MysqlTables(baseName + "_data", baseName + "_ranked", baseName + "_rank", baseName + "_rankd");
    }

    // path 存的是键 ID, 和 data_key 一样按字节比较, 键 ID 区分大小写
    @NotNull
    List<String> createStatements() {
        return List.of(
                """
                CREATE TABLE IF NOT EXISTS %s (
                    entity_id VARBINARY(64) NOT NULL,
                    path VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                    num BIGINT NULL,
                    val MEDIUMBLOB NULL,
                    dbl DOUBLE NULL,
                    PRIMARY KEY (entity_id, path)
                ) ENGINE = InnoDB""".formatted(this.data),
                """
                CREATE TABLE IF NOT EXISTS %s (
                    data_key VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                    entity_id VARBINARY(64) NOT NULL,
                    num BIGINT NULL,
                    dbl DOUBLE NULL,
                    PRIMARY KEY (entity_id, data_key),
                    INDEX %s (data_key, num, entity_id),
                    INDEX %s (data_key, dbl, entity_id)
                ) ENGINE = InnoDB""".formatted(this.ranked, this.rankIndex, this.rankDoubleIndex)
        );
    }

    @NotNull
    String upsertData() {
        return "INSERT INTO " + this.data + " (entity_id, path, num, val, dbl) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE num = VALUES(num), val = VALUES(val), dbl = VALUES(dbl)";
    }

    @NotNull
    String insertData() {
        return "INSERT INTO " + this.data + " (entity_id, path, val) VALUES (?, ?, ?)";
    }

    @NotNull
    String addData() {
        return "INSERT INTO " + this.data + " (entity_id, path, num) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE num = num + VALUES(num)";
    }

    @NotNull
    String selectRows() {
        return "SELECT path, num, val, dbl FROM " + this.data + " WHERE entity_id = ?";
    }

    @NotNull
    String selectRow() {
        return "SELECT num, val, dbl FROM " + this.data + " WHERE entity_id = ? AND path = ?";
    }

    @NotNull
    String lockRow() {
        return "SELECT num, val, dbl FROM " + this.data + " WHERE entity_id = ? AND path = ? FOR UPDATE";
    }

    @NotNull
    String compareAndSetData() {
        return "UPDATE " + this.data + " SET val = ? WHERE entity_id = ? AND path = ? AND val = ?";
    }

    @NotNull
    String compareAndRemoveData() {
        return "DELETE FROM " + this.data + " WHERE entity_id = ? AND path = ? AND val = ?";
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
        return "DELETE FROM " + this.data + " WHERE path = ? LIMIT " + limit;
    }

    @NotNull
    String upsertRanked() {
        return "INSERT INTO " + this.ranked + " (entity_id, data_key, num, dbl) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE num = VALUES(num), dbl = VALUES(dbl)";
    }

    @NotNull
    String addRanked() {
        return "INSERT INTO " + this.ranked + " (entity_id, data_key, num) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE num = num + VALUES(num)";
    }

    @NotNull
    String selectRankedRows() {
        return "SELECT data_key, num, dbl FROM " + this.ranked + " WHERE entity_id = ?";
    }

    @NotNull
    String selectRanked() {
        return "SELECT num, dbl FROM " + this.ranked + " WHERE entity_id = ? AND data_key = ?";
    }

    @NotNull
    String lockRanked() {
        return "SELECT num, dbl FROM " + this.ranked + " WHERE entity_id = ? AND data_key = ? FOR UPDATE";
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

    // 浮点键的排行走 (data_key, dbl, entity_id) 这个索引
    @NotNull
    String topDouble() {
        return "SELECT entity_id, dbl FROM " + this.ranked + " WHERE data_key = ? ORDER BY dbl DESC, entity_id DESC LIMIT ? OFFSET ?";
    }

    @NotNull
    String countAboveDouble() {
        return "SELECT COUNT(*) FROM " + this.ranked + " WHERE data_key = ? AND dbl > ?";
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