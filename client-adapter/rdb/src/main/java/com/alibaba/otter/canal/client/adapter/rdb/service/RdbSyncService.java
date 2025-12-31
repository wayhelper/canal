package com.alibaba.otter.canal.client.adapter.rdb.service;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter.Feature;
import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig.DbMapping;
import com.alibaba.otter.canal.client.adapter.rdb.support.BatchExecutor;
import com.alibaba.otter.canal.client.adapter.rdb.support.SingleDml;
import com.alibaba.otter.canal.client.adapter.rdb.support.SyncUtil;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.Util;

import javax.sql.DataSource;

/**
 * RDB同步操作业务
 *
 * @author rewerma 2018-11-7 下午06:45:49
 * @version 1.0.0
 */
public class RdbSyncService {

    private static final Logger               logger  = LoggerFactory.getLogger(RdbSyncService.class);
    private DataSource                        targetDS; // 源库

    private DruidDataSource                   dataSource; //目标库
    // 源库表字段类型缓存: instance.schema.table -> <columnName, jdbcType>
    private Map<String, Map<String, Integer>> columnsTypeCache;

    private int                               threads = 3;
    private boolean                           skipDupException;

    private List<SyncItem>[]                  dmlsPartition;
    private BatchExecutor[]                   batchExecutors;
    private ExecutorService[]                 executorThreads;

    public List<SyncItem>[] getDmlsPartition() {
        return dmlsPartition;
    }

    public Map<String, Map<String, Integer>> getColumnsTypeCache() {
        return columnsTypeCache;
    }

    public RdbSyncService(DruidDataSource dataSource,DataSource targetDS, Integer threads, boolean skipDupException){
        this(dataSource,targetDS, threads, new ConcurrentHashMap<>(), skipDupException);
    }

    @SuppressWarnings("unchecked")
    public RdbSyncService(DruidDataSource dataSource,DataSource targetDS, Integer threads, Map<String, Map<String, Integer>> columnsTypeCache,
                          boolean skipDupException){
        this.dataSource = dataSource;
        this.targetDS = targetDS;
        this.columnsTypeCache = columnsTypeCache;
        this.skipDupException = skipDupException;
        try {
            if (threads != null) {
                this.threads = threads;
            }
            this.dmlsPartition = new List[this.threads];
            this.batchExecutors = new BatchExecutor[this.threads];
            this.executorThreads = new ExecutorService[this.threads];
            for (int i = 0; i < this.threads; i++) {
                dmlsPartition[i] = new ArrayList<>();
                batchExecutors[i] = new BatchExecutor(dataSource);
                executorThreads[i] = Executors.newSingleThreadExecutor();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 批量同步回调
     *
     * @param dmls 批量 DML
     * @param function 回调方法
     */
    public void sync(List<Dml> dmls, Function<Dml, Boolean> function) {
        try {
            boolean toExecute = false;
            for (Dml dml : dmls) {
                if (!toExecute) {
                    toExecute = function.apply(dml);
                } else {
                    function.apply(dml);
                }
            }
            if (toExecute) {
                List<Future<Boolean>> futures = new ArrayList<>();
                for (int i = 0; i < threads; i++) {
                    int j = i;
                    if (dmlsPartition[j].isEmpty()) {
                        // bypass
                        continue;
                    }

                    futures.add(executorThreads[i].submit(() -> {
                        try {
                            dmlsPartition[j].forEach(syncItem -> sync(batchExecutors[j],
                                syncItem.config,
                                syncItem.singleDml));
                            dmlsPartition[j].clear();
                            batchExecutors[j].commit();
                            return true;
                        } catch (Throwable e) {
                            dmlsPartition[j].clear();
                            batchExecutors[j].rollback();
                            throw new RuntimeException(e);
                        }
                    }));
                }

                futures.forEach(future -> {
                    try {
                        future.get();
                    } catch (ExecutionException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        } finally {
            for (BatchExecutor batchExecutor : batchExecutors) {
                if (batchExecutor != null) {
                    batchExecutor.close();
                }
            }
        }
    }

    /**
     * 批量同步
     *
     * @param mappingConfig 配置集合
     * @param dmls 批量 DML
     */
    public void sync(Map<String, Map<String, MappingConfig>> mappingConfig, List<Dml> dmls, Properties envProperties) {
        sync(dmls, dml -> {
            if (dml.getIsDdl() != null && dml.getIsDdl() && StringUtils.isNotEmpty(dml.getSql())) {
                // DDL
            columnsTypeCache.remove(dml.getDestination() + "." + dml.getDatabase() + "." + dml.getTable());
            return false;
        } else {
            // DML
            String destination = StringUtils.trimToEmpty(dml.getDestination());
            String groupId = StringUtils.trimToEmpty(dml.getGroupId());
            String database = dml.getDatabase();
            String table = dml.getTable();
            Map<String, MappingConfig> configMap;
            if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
                configMap = mappingConfig.get(destination + "-" + groupId + "_" + database + "-" + table);
            } else {
                configMap = mappingConfig.get(destination + "_" + database + "-" + table);
            }

            if (configMap == null) {
                return false;
            }

            if (configMap.values().isEmpty()) {
                return false;
            }

            for (MappingConfig config : configMap.values()) {
                appendDmlPartition(config, dml);
            }
            return true;
        }
    }   );
    }

    /**
     * 将Dml加入 {@link #dmlsPartition}
     *
     * @param config 表映射配置
     * @param dml    Dml对象
     */
    public void appendDmlPartition(MappingConfig config, Dml dml) {
        boolean caseInsensitive = config.getDbMapping().isCaseInsensitive();
        if (config.getConcurrent()) {
            List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml, caseInsensitive);
            singleDmls.forEach(singleDml -> {
                int hash = pkHash(config.getDbMapping(), singleDml.getData());
                SyncItem syncItem = new SyncItem(config, singleDml);
                dmlsPartition[hash].add(syncItem);
            });
        } else {
            int hash = 0;
            List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml, caseInsensitive);
            singleDmls.forEach(singleDml -> {
                SyncItem syncItem = new SyncItem(config, singleDml);
                dmlsPartition[hash].add(syncItem);
            });
        }
    }

    /**
     * 单条 dml 同步
     *
     * @param batchExecutor 批量事务执行器
     * @param config 对应配置对象
     * @param dml DML
     */
    /*public void sync(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) {
        if (config != null) {
            try {
                String type = dml.getType();
                if (type != null && type.equalsIgnoreCase("INSERT")) {
                    insert(batchExecutor, config, dml);
                } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
                    update(batchExecutor, config, dml);
                } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                    delete(batchExecutor, config, dml);
                } else if (type != null && type.equalsIgnoreCase("TRUNCATE")) {
                    truncate(batchExecutor, config);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("DML: {}", JSON.toJSONString(dml, Feature.WriteNulls));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }*/
    public void sync(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) {
        String type = dml.getType();
        // 获取你在 yml 中配置的自定义 SQL
        String customSql = config.getDbMapping().getCustomUpsertSql();

        if (customSql != null && (type.equalsIgnoreCase("INSERT") || type.equalsIgnoreCase("UPDATE"))) {
            // 1. 从当前 dml 提取主键值,若提取复合主键值，保持顺序
            Map<String, String> targetPkMap = config.getDbMapping().getTargetPk();
            List<Object> pkValues = new ArrayList<>(targetPkMap.size());

            for (String targetPkField : targetPkMap.values()) {
                Object val = dml.getData().get(targetPkField);
                pkValues.add(val);
            }
            // 2. 去 MySQL 反查最新数据 (需实现 queryFromMysql 方法)
            Map<String, Object> latestData = queryFromSource(customSql, pkValues);

            if (latestData != null) {
                // 3. 将反查结果同步到 Oracle (调用下面提到的 mergeInto 方法)
                oracleMergeUpdate(batchExecutor, config, latestData);
                logger.debug("Affected Oracle via custom upsert SQL for PK: {}", pkValues);
            }
        } else if (type.equalsIgnoreCase("DELETE")) {
            try {
                delete(batchExecutor, config, dml); // 删除逻辑保持不变，默认用主键
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 反查源库
     * @param sql
     * @param pkValue
     * @return
     */
    /**
     * 反查源库（支持复合主键）
     * SQL 示例：
     * SELECT * FROM t WHERE pk1 = ? AND pk2 = ? AND pk3 = ?
     */
    private Map<String, Object> queryFromSource(String sql, List<Object> pkValues) {
        try (Connection conn = targetDS.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // 按顺序绑定所有主键
            for (int i = 0; i < pkValues.size(); i++) {
                pstmt.setObject(i + 1, pkValues.get(i));
            }

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return SyncUtil.resultSetToMap(rs);
                }
            }
        } catch (SQLException e) {
            logger.error("反查源库失败, SQL: {}, PKs: {}", sql, pkValues, e);
        }
        return null;
    }

    private void oracleMergeUpdate(BatchExecutor batchExecutor, MappingConfig config, Map<String, Object> latestData) {
        DbMapping dbMapping = config.getDbMapping();
        String targetTable = dbMapping.getTargetTable();
        // 动态获取目标库主键配置 Map (例如 { "cid": "cid" })
        Map<String, String> targetPk = dbMapping.getTargetPk();
        // 获取字段映射关系
        Map<String, String> columnsMap = SyncUtil.getColumnsMap(dbMapping, latestData);
        // 获取目标库（Oracle）字段类型缓存
        Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getConn(), config);

        StringBuilder sql = new StringBuilder();
        sql.append("MERGE INTO ").append(targetTable).append(" t ");
        sql.append("USING (SELECT ");

        List<Map<String, ?>> values = new ArrayList<>();

        // 1. 构建 USING 部分：将反查的数据转为虚表源 (s)
        columnsMap.forEach((targetCol, srcCol) -> {
            sql.append("? AS ").append(targetCol).append(",");
            Integer type = ctype.get(targetCol.toLowerCase());
            BatchExecutor.setValue(values, type, latestData.get(srcCol));
        });
        sql.deleteCharAt(sql.length() - 1).append(" FROM DUAL) s ");

        // 2. 构建 ON 部分：根据 targetPk 动态生成关联条件
        sql.append("ON (");
        // 遍历 targetPk 的所有键（目标字段名），如 cid
        targetPk.keySet().forEach(pkName -> {
            sql.append("t.").append(pkName).append(" = s.").append(pkName).append(" AND ");
        });
        sql.delete(sql.length() - 5, sql.length()).append(") "); // 移除多余的 AND

        // 3. 构建更新部分 (WHEN MATCHED)
        sql.append("WHEN MATCHED THEN UPDATE SET ");
        columnsMap.keySet().forEach(col -> {
            // 关键逻辑：只有当该列不属于 targetPk 时才允许更新
            if (!targetPk.containsKey(col)) {
                sql.append("t.").append(col).append(" = s.").append(col).append(",");
            }
        });
        sql.deleteCharAt(sql.length() - 1);

        // 4. 构建插入部分 (WHEN NOT MATCHED)
        sql.append(" WHEN NOT MATCHED THEN INSERT (");
        columnsMap.keySet().forEach(col -> sql.append(col).append(","));
        sql.deleteCharAt(sql.length() - 1).append(") VALUES (");
        columnsMap.keySet().forEach(col -> sql.append("s.").append(col).append(","));
        sql.deleteCharAt(sql.length() - 1).append(")");

        // 5. 执行同步
        try {
            batchExecutor.execute(sql.toString(), values);
            if (logger.isDebugEnabled()) {
                logger.debug("Oracle Merge SQL Executed: {}", sql.toString());
            }
        } catch (SQLException e) {
            logger.error("Oracle Merge Error! Table: {}, SQL: {}", targetTable, sql.toString());
            throw new RuntimeException(e);
        }
    }

    /**
     * 插入操作
     *
     * @param config 配置项
     * @param dml DML数据
     */
    private void insert(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) throws SQLException {
        Map<String, Object> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        DbMapping dbMapping = config.getDbMapping();
        String backtick = SyncUtil.getBacktickByDbType(dataSource.getDbType());
        Map<String, String> columnsMap = SyncUtil.getColumnsMap(dbMapping, data);

        StringBuilder insertSql = new StringBuilder();
        insertSql.append("INSERT INTO ").append(SyncUtil.getDbTableName(dbMapping, dataSource.getDbType())).append(" (");

        columnsMap.forEach((targetColumnName, srcColumnName) -> insertSql.append(backtick)
            .append(targetColumnName)
            .append(backtick)
            .append(","));
        int len = insertSql.length();
        insertSql.delete(len - 1, len).append(") VALUES (");
        int mapLen = columnsMap.size();
        for (int i = 0; i < mapLen; i++) {
            insertSql.append("?,");
        }
        len = insertSql.length();
        insertSql.delete(len - 1, len).append(")");

        Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getConn(), config);

        List<Map<String, ?>> values = new ArrayList<>();
        for (Map.Entry<String, String> entry : columnsMap.entrySet()) {
            String targetColumnName = entry.getKey();
            String srcColumnName = entry.getValue();
            if (srcColumnName == null) {
                srcColumnName = Util.cleanColumn(targetColumnName);
            }

            Integer type = ctype.get(Util.cleanColumn(targetColumnName).toLowerCase());
            if (type == null) {
                throw new RuntimeException("Target column: " + targetColumnName + " not matched");
            }
            Object value = data.get(srcColumnName);
            BatchExecutor.setValue(values, type, value);
        }

        try {
            batchExecutor.execute(insertSql.toString(), values);
        } catch (SQLException e) {
            if (skipDupException
                && (e.getMessage().contains("Duplicate entry") || e.getMessage().contains("duplicate key") || e.getMessage().startsWith("ORA-00001:"))) {
                // ignore
                // TODO 增加更多关系数据库的主键冲突的错误码
            } else {
                throw e;
            }
        }
        if (logger.isTraceEnabled()) {
            logger.trace("Insert into target table, sql: {}", insertSql);
        }

    }

    /**
     * 更新操作
     *
     * @param config 配置项
     * @param dml DML数据
     */
    private void update(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) throws SQLException {
        Map<String, Object> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        Map<String, Object> old = dml.getOld();
        if (old == null || old.isEmpty()) {
            return;
        }

        DbMapping dbMapping = config.getDbMapping();
        String backtick = SyncUtil.getBacktickByDbType(dataSource.getDbType());
        Map<String, String> columnsMap = SyncUtil.getColumnsMap(dbMapping, data);

        Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getConn(), config);

        StringBuilder updateSql = new StringBuilder();
        updateSql.append("UPDATE ").append(SyncUtil.getDbTableName(dbMapping, dataSource.getDbType())).append(" SET ");
        List<Map<String, ?>> values = new ArrayList<>();
        boolean hasMatched = false;
        for (String srcColumnName : old.keySet()) {
            List<String> targetColumnNames = new ArrayList<>();
            columnsMap.forEach((targetColumn, srcColumn) -> {
                if (srcColumnName.equalsIgnoreCase(srcColumn)) {
                    targetColumnNames.add(targetColumn);
                }
            });
            if (!targetColumnNames.isEmpty()) {
                hasMatched = true;
                for (String targetColumnName : targetColumnNames) {
                    updateSql.append(backtick).append(targetColumnName).append(backtick).append("=?, ");
                    Integer type = ctype.get(Util.cleanColumn(targetColumnName).toLowerCase());
                    if (type == null) {
                        throw new RuntimeException("Target column: " + targetColumnName + " not matched");
                    }
                    BatchExecutor.setValue(values, type, data.get(srcColumnName));
                }
            }
        }
        if (!hasMatched) {
            logger.warn("Did not matched any columns to update ");
            return;
        }
        int len = updateSql.length();
        updateSql.delete(len - 2, len).append(" WHERE ");

        // 拼接主键
        appendCondition(dbMapping, updateSql, ctype, values, data, old);
        batchExecutor.execute(updateSql.toString(), values);
        if (logger.isTraceEnabled()) {
            logger.trace("Update target table, sql: {}", updateSql);
        }
    }

    /**
     * 删除操作
     *
     * @param config
     * @param dml
     */
    private void delete(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) throws SQLException {
        Map<String, Object> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        DbMapping dbMapping = config.getDbMapping();
        Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getConn(), config);

        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ").append(SyncUtil.getDbTableName(dbMapping, dataSource.getDbType())).append(" WHERE ");

        List<Map<String, ?>> values = new ArrayList<>();
        // 拼接主键
        appendCondition(dbMapping, sql, ctype, values, data);
        batchExecutor.execute(sql.toString(), values);
        if (logger.isTraceEnabled()) {
            logger.trace("Delete from target table, sql: {}", sql);
        }
    }

    /**
     * truncate操作
     *
     * @param config
     */
    private void truncate(BatchExecutor batchExecutor, MappingConfig config) throws SQLException {
        DbMapping dbMapping = config.getDbMapping();
        StringBuilder sql = new StringBuilder();
        sql.append("TRUNCATE TABLE ").append(SyncUtil.getDbTableName(dbMapping, dataSource.getDbType()));
        batchExecutor.execute(sql.toString(), new ArrayList<>());
        if (logger.isTraceEnabled()) {
            logger.trace("Truncate target table, sql: {}", sql);
        }
    }

    /**
     * 获取目标字段类型
     *
     * @param conn sql connection
     * @param config 映射配置
     * @return 字段sqlType
     */
    private Map<String, Integer> getTargetColumnType(Connection conn, MappingConfig config) {
        DbMapping dbMapping = config.getDbMapping();
        String cacheKey = config.getDestination() + "." + dbMapping.getDatabase() + "." + dbMapping.getTable();
        Map<String, Integer> columnType = columnsTypeCache.get(cacheKey);
        if (columnType == null) {
            synchronized (RdbSyncService.class) {
                columnType = columnsTypeCache.get(cacheKey);
                if (columnType == null) {
                    columnType = new LinkedHashMap<>();
                    final Map<String, Integer> columnTypeTmp = columnType;
                    String sql = "SELECT * FROM " + SyncUtil.getDbTableName(dbMapping, dataSource.getDbType()) + " WHERE 1=2";
                    Util.sqlRS(conn, sql, rs -> {
                        try {
                            ResultSetMetaData rsd = rs.getMetaData();
                            int columnCount = rsd.getColumnCount();
                            for (int i = 1; i <= columnCount; i++) {
                                int colType = rsd.getColumnType(i);
                                // 修复year类型作为date处理时的data truncated问题
                                if ("YEAR".equals(rsd.getColumnTypeName(i))) {
                                    colType = Types.VARCHAR;
                                }
                                columnTypeTmp.put(rsd.getColumnName(i).toLowerCase(), colType);
                            }
                            columnsTypeCache.put(cacheKey, columnTypeTmp);
                        } catch (SQLException e) {
                            logger.error(e.getMessage(), e);
                        }
                    });
                }
            }
        }
        return columnType;
    }

    /**
     * 拼接主键 where条件
     */
    private void appendCondition(MappingConfig.DbMapping dbMapping, StringBuilder sql, Map<String, Integer> ctype,
                                 List<Map<String, ?>> values, Map<String, Object> d) {
        appendCondition(dbMapping, sql, ctype, values, d, null);
    }

    private void appendCondition(MappingConfig.DbMapping dbMapping, StringBuilder sql, Map<String, Integer> ctype,
                                 List<Map<String, ?>> values, Map<String, Object> d, Map<String, Object> o) {
        String backtick = SyncUtil.getBacktickByDbType(dataSource.getDbType());

        // 拼接主键
        for (Map.Entry<String, String> entry : dbMapping.getTargetPk().entrySet()) {
            String targetColumnName = entry.getKey();
            String srcColumnName = entry.getValue();
            if (srcColumnName == null) {
                srcColumnName = Util.cleanColumn(targetColumnName);
            }
            sql.append(backtick).append(targetColumnName).append(backtick).append("=? AND ");
            Integer type = ctype.get(Util.cleanColumn(targetColumnName).toLowerCase());
            if (type == null) {
                throw new RuntimeException("Target column: " + targetColumnName + " not matched");
            }
            // 如果有修改主键的情况
            if (o != null && o.containsKey(srcColumnName)) {
                BatchExecutor.setValue(values, type, o.get(srcColumnName));
            } else {
                BatchExecutor.setValue(values, type, d.get(srcColumnName));
            }
        }
        int len = sql.length();
        sql.delete(len - 4, len);
    }

    public static class SyncItem {

        private MappingConfig config;
        private SingleDml     singleDml;

        public SyncItem(MappingConfig config, SingleDml singleDml){
            this.config = config;
            this.singleDml = singleDml;
        }
    }

    /**
     * 取主键hash
     */
    public int pkHash(DbMapping dbMapping, Map<String, Object> d) {
        return pkHash(dbMapping, d, null);
    }

    public int pkHash(DbMapping dbMapping, Map<String, Object> d, Map<String, Object> o) {
        int hash = 0;
        // 取主键
        for (Map.Entry<String, String> entry : dbMapping.getTargetPk().entrySet()) {
            String targetColumnName = entry.getKey();
            String srcColumnName = entry.getValue();
            if (srcColumnName == null) {
                srcColumnName = Util.cleanColumn(targetColumnName);
            }
            Object value = null;
            if (o != null && o.containsKey(srcColumnName)) {
                value = o.get(srcColumnName);
            } else if (d != null) {
                value = d.get(srcColumnName);
            }
            if (value != null) {
                hash += value.hashCode();
            }
        }
        hash = Math.abs(hash) % threads;
        return Math.abs(hash);
    }

    public void close() {
        for (int i = 0; i < threads; i++) {
            executorThreads[i].shutdown();
        }
    }
}
