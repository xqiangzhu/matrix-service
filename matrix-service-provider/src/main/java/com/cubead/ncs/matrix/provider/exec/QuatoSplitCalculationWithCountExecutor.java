package com.cubead.ncs.matrix.provider.exec;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Component;

import com.cubead.ncs.matrix.api.Dimension;
import com.cubead.ncs.matrix.api.DubboResult.ResultStatus;
import com.cubead.ncs.matrix.api.Quota;
import com.cubead.ncs.matrix.api.QuotaWithValue;
import com.cubead.ncs.matrix.api.SqlDismantling;
import com.cubead.ncs.matrix.api.SqlDismantling.QueryUnit;
import com.cubead.ncs.matrix.provider.tools.PartitionSqlGenerator;

/**
 * 分表执行引擎
 * 
 * @author kangye
 */
@Component
public class QuatoSplitCalculationWithCountExecutor {

    private static final Logger logger = LoggerFactory.getLogger(QuatoSplitCalculationWithCountExecutor.class);

    @Autowired
    JdbcTemplate jdbcTemplate;

    // 单个任务计算线程池数设置过大会迅速耗尽数据库连接池资源
    private static ExecutorService executorService = new ThreadPoolExecutor(30, 100, 10, TimeUnit.SECONDS,
            new LinkedBlockingDeque<Runnable>(), new ConcereteThreadFactory());

    static class ConcereteUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
        @Override
        public void uncaughtException(Thread thread, Throwable exception) {
            System.out.println("thread id:" + thread.getId() + " name:" + thread.getName() + " exception_message:"
                    + exception.getMessage());
        }

    }

    static class ConcereteThreadFactory implements ThreadFactory {

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable);
            thread.setUncaughtExceptionHandler(new ConcereteUncaughtExceptionHandler());
            return thread;

        }

    }

    public RowMergeResultSet calculatAllMergeResultSet(Boolean partitionSupport, QueryUnit... quotaunits) {

        if (quotaunits == null)
            return null;

        RowMergeResultSet rowMergeResultSet = new RowMergeResultSet();
        SqlDismantling[] sqlDismantlings = rowMergeResultSet.validateQueryUnitGroup(quotaunits);

        final CountDownLatch latch = new CountDownLatch(quotaunits.length);

        for (int i = 0; i < sqlDismantlings.length; i++) {
            // 选择单表查询策略
            CalculatSqlRowTaskWithCount calculatSqlRowTaskWithCount = partitionSupport ? new CalculatSqlRowTaskWithCountInPartitionStrategy(
                    sqlDismantlings[i], rowMergeResultSet, latch)
                    : new CalculatSqlRowTaskWithCountInWholeTableStrategy(sqlDismantlings[i], rowMergeResultSet, latch);
            executorService.execute(calculatSqlRowTaskWithCount);
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException("运行异常:" + e.getMessage());
        }

        return rowMergeResultSet;

    }

    abstract class CalculatSqlRowTaskWithCount implements Runnable {

        private RowMergeResultSet rowMergeResultSet;
        private CountDownLatch latch;
        private SqlDismantling sqlDismantling;

        public CalculatSqlRowTaskWithCount(SqlDismantling sqlDismantling, RowMergeResultSet rowMergeResultSet,
                CountDownLatch latch) {
            super();
            this.rowMergeResultSet = rowMergeResultSet;
            this.latch = latch;
            this.sqlDismantling = sqlDismantling;
        }

        // 执行sql计算
        protected void queryResultAndComniedCalculat(final Dimension dimension, final long current_time,
                final String sql) {
            jdbcTemplate.query(sql, new ResultSetExtractor<Object>() {
                public Object extractData(ResultSet resultSet) throws SQLException, DataAccessException {
                    combinedCalculatResult(dimension, resultSet, current_time, sql);
                    return null;
                }
            });
        }

        // 合并计算结果
        protected void combinedCalculatResult(final Dimension dimension, ResultSet resultSet, final long current_time,
                final String sql) throws SQLException {
            while (resultSet.next()) {
                dimension.inizValues(resultSet);
                SQLRowResultMapping sqlRowResultMapping = new SQLRowResultMapping(dimension);
                List<QuotaWithValue> quotaWithValues = new ArrayList<>();
                for (Quota quota : sqlDismantling.getQuotas()) {
                    QuotaWithValue quotaWithValue = new QuotaWithValue(quota);
                    quotaWithValue.setValue(resultSet.getDouble(quota.getQuota()));
                    quotaWithValues.add(quotaWithValue);
                }
                sqlRowResultMapping.setQuotaWithValues(quotaWithValues);
                rowMergeResultSet.addRowMergeResultWithAllCount(sqlRowResultMapping, sqlDismantling.getIsOrderUnit());
            }
            logger.debug("{}查询耗时：{}ms", sql, (System.currentTimeMillis() - current_time));

        }

        // 异常处理
        protected void handleExceptionRecode(final String partitionSql, Exception e) {
            logger.error("{}执行异常:{}", partitionSql, e);
            synchronized (rowMergeResultSet) {
                rowMergeResultSet.getDubboResult().setMessageAndStatus(e.getMessage(), ResultStatus.FAIL);
            }
        }

        public void run() {

            final Dimension dimension = new Dimension(sqlDismantling.getFields());
            final String sql = sqlDismantling.getQueryUnit().getSql();
            final long current_time = System.currentTimeMillis();

            try {
                execute(dimension, sql, current_time);
            } catch (Exception e) {
                handleExceptionRecode(sql, e);
            } finally {
                latch.countDown();
            }
        }

        abstract void execute(final Dimension dimension, final String sql, final long current_time) throws Exception;
    }

    /**
     * 全表直接执行,更少的连接数消耗
     * 
     * @author kangye
     */
    class CalculatSqlRowTaskWithCountInWholeTableStrategy extends CalculatSqlRowTaskWithCount {

        public CalculatSqlRowTaskWithCountInWholeTableStrategy(SqlDismantling sqlDismantling,
                RowMergeResultSet rowMergeResultSet, CountDownLatch latch) {
            super(sqlDismantling, rowMergeResultSet, latch);
        }

        @Override
        void execute(Dimension dimension, String sql, long current_time) {
            queryResultAndComniedCalculat(dimension, current_time, sql);
        }

    }

    /**
     * 重新拼接sql，分区并行计算，更快的查询效率
     * 
     * @author kangye
     */
    class CalculatSqlRowTaskWithCountInPartitionStrategy extends CalculatSqlRowTaskWithCount {

        public CalculatSqlRowTaskWithCountInPartitionStrategy(SqlDismantling sqlDismantling,
                RowMergeResultSet rowMergeResultSet, CountDownLatch latch) {
            super(sqlDismantling, rowMergeResultSet, latch);
        }

        @Override
        void execute(final Dimension dimension, final String sql, final long current_time) throws Exception {
            // 分拆成分区查询语句
            final String[] parationSqls = PartitionSqlGenerator.generatPartitionSql(sql);
            if (null == parationSqls || parationSqls.length <= 1) {
                // 仍然在一个分区上
                queryResultAndComniedCalculat(dimension, current_time, sql);
            } else {
                final CountDownLatch subCountDownLatch = new CountDownLatch(parationSqls.length);
                for (int i = 0; i < parationSqls.length; i++) {
                    final String partitionSql = parationSqls[i];
                    executorService.execute(new Runnable() {
                        public void run() {
                            try {
                                queryResultAndComniedCalculat(dimension, current_time, partitionSql);
                            } catch (Exception e) {
                                handleExceptionRecode(partitionSql, e);
                            } finally {
                                subCountDownLatch.countDown();
                            }
                        }
                    });

                }
                subCountDownLatch.await();
            }
        }

    }
}
