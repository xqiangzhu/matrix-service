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
import com.cubead.ncs.matrix.provider.tools.Contants;

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

    public RowMergeResultSet calculatAllMergeResultSet(QueryUnit... quotaunits) {

        if (quotaunits == null)
            return null;

        RowMergeResultSet rowMergeResultSet = new RowMergeResultSet();
        SqlDismantling[] sqlDismantlings = rowMergeResultSet.validateQueryUnitGroup(quotaunits);

        final CountDownLatch latch = new CountDownLatch(quotaunits.length);

        for (int i = 0; i < sqlDismantlings.length; i++) {
            executorService.execute(new CalculatSqlRowTaskWithCount(sqlDismantlings[i], rowMergeResultSet, latch));
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException("运行异常:" + e.getMessage());
        }

        return rowMergeResultSet;

    }

    class CalculatSqlRowTaskWithCount implements Runnable {

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

        private void queryResultAndComniedCalculat(final Dimension dimension, final long current_time,
                final String partitionSql) {
            jdbcTemplate.query(partitionSql, new ResultSetExtractor<Object>() {
                public Object extractData(ResultSet resultSet) throws SQLException, DataAccessException {
                    logger.debug("{}查询耗时：{}ms", partitionSql, (System.currentTimeMillis() - current_time));
                    combinedCalculatResult(dimension, resultSet);
                    return null;
                }
            });
        }

        private void combinedCalculatResult(final Dimension dimension, ResultSet resultSet) throws SQLException {
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

        }

        public void run() {

            final Dimension dimension = new Dimension(sqlDismantling.getFields());
            final String sql = sqlDismantling.getQueryUnit().getSql();
            final long current_time = System.currentTimeMillis();

            try {
                // 分拆成分区查询语句
                final String[] parationSqls = SqlRandomGenerator.generatPartitionSql(sql);

                if (!"on".equals(Contants.PARTITION_PARALLEL_COMPUTING_SUPPORT) || parationSqls.length == 1) {
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
                                    queryExceptionRecode(partitionSql, e);
                                } finally {
                                    subCountDownLatch.countDown();
                                }
                            }
                        });

                    }
                    subCountDownLatch.await();

                }

            } catch (Exception e) {
                queryExceptionRecode(sql, e);
            } finally {
                latch.countDown();
            }
        }

        private void queryExceptionRecode(final String partitionSql, Exception e) {
            logger.error("{}执行异常:{}", partitionSql, e);
            synchronized (rowMergeResultSet) {
                rowMergeResultSet.getDubboResult().setMessageAndStatus(e.getMessage(), ResultStatus.FAIL);
            }
        }
    }

}
