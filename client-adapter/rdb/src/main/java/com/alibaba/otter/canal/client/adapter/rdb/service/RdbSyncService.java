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
    public void sync(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) {
        String type = dml.getType();
        // 获取你在 yml 中配置的自定义 SQL
        String customSql = config.getDbMapping().getCustomUpsertSql();

        if (customSql != null && (type.equalsIgnoreCase("INSERT") || type.equalsIgnoreCase("UPDATE"))) {
            // 1. 从当前 dml 提取主键值,若提取复合主键值，保持顺序
            Map<String, String> targetPkMap = config.getDbMapping().getTargetPk();
            List<Object> pkValues = new ArrayList<>(targetPkMap.size());

            for (String targetPkColumn : targetPkMap.values()) {
                Object val = dml.getData().get(targetPkColumn);
                if (val == null) {
                    // 预防性检查：如果 DML 数据中缺少主键列，记录警告
                    logger.warn("主键列 [{}] 在 DML 数据中不存在", targetPkColumn);
                }
                pkValues.add(val);
            }
            // 2. 去 MySQL 反查最新数据 (需实现 queryFromMysql 方法)
            Map<String, Object> latestData = queryFromSource(customSql, pkValues);
            if (latestData != null && type.equalsIgnoreCase("INSERT")) {
                // 传入反查到的最新数据进行插入
                insertFromSource(batchExecutor, config, latestData);
            } else if (latestData != null && type.equalsIgnoreCase("UPDATE")) {
                // 传入反查到的最新数据进行更新
                updateFromSource(batchExecutor, config, dml, latestData);
            } else {
                // 4. 若反查结果为空,先跳过并打印日志
                logger.warn("反查结果为空,跳过同步. type={}, pkValues={}", type, JSON.toJSONString(pkValues, Feature.WriteNulls));
            }
        } else if (type.equalsIgnoreCase("DELETE")) {
            try {
                delete(batchExecutor, config, dml);
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
                pstmt.setObject(i + 1, pkValues.get(pkValues.size()-(i+1)));
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

    private void insertFromSource(BatchExecutor batchExecutor, MappingConfig config, Map<String, Object> latestData) {
        DbMapping dbMapping = config.getDbMapping();
        // 获取目标库（Oracle）的字段类型缓存
        Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getConn(), config);

        // 构建 SQL
        StringBuilder insertSql = new StringBuilder();
        insertSql.append("INSERT INTO ").append(SyncUtil.getDbTableName(dbMapping, dataSource.getDbType())).append(" (");

        // 映射字段：targetColumn -> sourceColumn
        Map<String, String> columnsMap = SyncUtil.getColumnsMap(dbMapping, latestData);
        List<Map<String, ?>> values = new ArrayList<>();

        StringJoiner columns = new StringJoiner(",");
        StringJoiner placeholders = new StringJoiner(",");

        for (Map.Entry<String, String> entry : columnsMap.entrySet()) {
            String targetColumnName = entry.getKey();
            String srcColumnName = entry.getValue() != null ? entry.getValue() : Util.cleanColumn(targetColumnName);

            // 拼接 SQL 部分
            columns.add(targetColumnName);
            placeholders.add("?");

            // 设置参数值
            Integer type = ctype.get(Util.cleanColumn(targetColumnName).toLowerCase());
            if (type == null) {
                throw new RuntimeException("Oracle target column: " + targetColumnName + " not found in metadata");
            }
            Object value = latestData.get(srcColumnName);
            BatchExecutor.setValue(values, type, value);
        }

        insertSql.append(columns.toString()).append(") VALUES (").append(placeholders.toString()).append(")");

        try {
            batchExecutor.execute(insertSql.toString(), values);
        } catch (SQLException e) {
            // 针对 Oracle 的主键冲突异常 (ORA-00001) 进行处理
            if (skipDupException && e.getMessage().contains("ORA-00001")) {
                logger.warn("Oracle 主键冲突，已跳过: {}", e.getMessage());
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private void updateFromSource(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml, Map<String, Object> latestData) {
        DbMapping dbMapping = config.getDbMapping();
        // 统一转为小写获取类型映射
        Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getConn(), config);
        // 映射关系：TargetColumn -> SourceColumn
        Map<String, String> columnsMap = SyncUtil.getColumnsMap(dbMapping, latestData);

        StringBuilder updateSql = new StringBuilder();
        updateSql.append("UPDATE ").append(SyncUtil.getDbTableName(dbMapping, dataSource.getDbType())).append(" SET ");

        List<Map<String, ?>> values = new ArrayList<>();
        StringJoiner setClauses = new StringJoiner(", ");

        // 1. 构建 SET 部分：必须排除主键字段
        for (Map.Entry<String, String> entry : columnsMap.entrySet()) {
            String targetCol = entry.getKey();
            String srcCol = entry.getValue() != null ? entry.getValue() : Util.cleanColumn(targetCol);

            // 获取主键配置（注意大小写敏感度）
            boolean isPk = dbMapping.getTargetPk().containsKey(targetCol);

            if (!isPk) {
                setClauses.add(targetCol + "=?");
                // 统一使用清洗后的名称去获取 JDBC 类型
                Integer type = ctype.get(Util.cleanColumn(targetCol).toLowerCase());
                if (type == null) {
                    throw new RuntimeException("Target column: " + targetCol + " not found in metadata");
                }
                BatchExecutor.setValue(values, type, latestData.get(srcCol));
            }
        }

        if (values.isEmpty()) {
            logger.warn("没有检测到需要更新的非主键字段，跳过更新。Table: {}", dbMapping.getTable());
            return;
        }

        updateSql.append(setClauses.toString()).append(" WHERE ");

        // 2. 构建 WHERE 条件：追加主键及其对应的值
        // appendCondition 会负责把主键值按顺序 add 到 values 列表末尾
        appendCondition(dbMapping, updateSql, ctype, values, latestData);

        try {
            batchExecutor.execute(updateSql.toString(), values);
        } catch (SQLException e) {
            throw new RuntimeException("Update failed for table " + dbMapping.getTable(), e);
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
