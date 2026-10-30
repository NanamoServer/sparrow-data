package net.momirealms.sparrow.data.storage.sql.postgresql;

import org.jetbrains.annotations.NotNull;

import java.util.List;

record PostgresTables(@NotNull String data, @NotNull String ranked, @NotNull String rankIndex) {

    // 一个实体类型的两张表. PostgreSQL 的索引名在整个 schema 里唯一, 所以索引名也带上前缀和类型名, 最长 61 个字符
    @NotNull
    static PostgresTables of(@NotNull String baseName) {
        return new PostgresTables(baseName + "_data", baseName + "_ranked", baseName + "_rank");
    }

    // path 和 data_key 按字节比较, PathCodec 的条目范围查询依赖这个排序
    @NotNull
    List<String> createStatements() {
        return List.of(
                """
                CREATE TABLE IF NOT EXISTS %s (
                    entity_id BYTEA NOT NULL,
                    path VARCHAR(255) COLLATE "C" NOT NULL,
                    num BIGINT NULL,
                    val BYTEA NULL,
                    PRIMARY KEY (entity_id, path)
                )""".formatted(this.data),
                """
                CREATE TABLE IF NOT EXISTS %s (
                    data_key VARCHAR(64) COLLATE "C" NOT NULL,
                    entity_id BYTEA NOT NULL,
                    num BIGINT NOT NULL,
                    PRIMARY KEY (entity_id, data_key)
                )""".formatted(this.ranked),
                "CREATE INDEX IF NOT EXISTS " + this.rankIndex + " ON " + this.ranked + " (data_key, num, entity_id)"
        );
    }

    @NotNull
    String upsertData() {
        return "INSERT INTO " + this.data + " (entity_id, path, num, val) VALUES (?, ?, ?, ?) ON CONFLICT (entity_id, path) DO UPDATE SET num = EXCLUDED.num, val = EXCLUDED.val";
    }

    @NotNull
    String insertData() {
        return "INSERT INTO " + this.data + " (entity_id, path, val) VALUES (?, ?, ?) ON CONFLICT (entity_id, path) DO NOTHING";
    }

    @NotNull
    String addData() {
        return "INSERT INTO " + this.data + " (entity_id, path, num) VALUES (?, ?, ?) ON CONFLICT (entity_id, path) DO UPDATE SET num = " + this.data + ".num + EXCLUDED.num RETURNING num";
    }

    @NotNull
    String takeData() {
        return "UPDATE " + this.data + " SET num = num - ? WHERE entity_id = ? AND path = ? AND num >= ? RETURNING num";
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
    String deleteRow() {
        return "DELETE FROM " + this.data + " WHERE entity_id = ? AND path = ?";
    }

    @NotNull
    String deleteRows() {
        return "DELETE FROM " + this.data + " WHERE entity_id = ?";
    }

    // PostgreSQL 的 DELETE 不支持 LIMIT, 用行的物理位置 ctid 先选出一批再删
    @NotNull
    String purgeDataBatch(int limit) {
        return "DELETE FROM " + this.data + " WHERE ctid IN (SELECT ctid FROM " + this.data + " WHERE path = ? OR (path >= ? AND path < ?) LIMIT " + limit + ")";
    }

    @NotNull
    String upsertRanked() {
        return "INSERT INTO " + this.ranked + " (entity_id, data_key, num) VALUES (?, ?, ?) ON CONFLICT (entity_id, data_key) DO UPDATE SET num = EXCLUDED.num";
    }

    @NotNull
    String addRanked() {
        return "INSERT INTO " + this.ranked + " (entity_id, data_key, num) VALUES (?, ?, ?) ON CONFLICT (entity_id, data_key) DO UPDATE SET num = " + this.ranked + ".num + EXCLUDED.num RETURNING num";
    }

    @NotNull
    String takeRanked() {
        return "UPDATE " + this.ranked + " SET num = num - ? WHERE entity_id = ? AND data_key = ? AND num >= ? RETURNING num";
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
        return "DELETE FROM " + this.ranked + " WHERE ctid IN (SELECT ctid FROM " + this.ranked + " WHERE data_key = ? LIMIT " + limit + ")";
    }
}