package com.cubead.ncs.matrix.provider.exec;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
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
import com.cubead.ncs.matrix.api.Quota;
import com.cubead.ncs.matrix.api.QuotaWithValue;
import com.cubead.ncs.matrix.api.SqlDismantling;
import com.cubead.ncs.matrix.api.SqlDismantling.QueryUnit;

/**
 * 分表执行引擎
 * 
 * @author kangye
 */
@Component
public class QuatoSplitCalculationExecutor {

    private static final Logger logger = LoggerFactory.getLogger(MatrixTableSearch.class);

    private static ExecutorService executorService = new ThreadPoolExecutor(10, 30, 10, TimeUnit.SECONDS,
            new LinkedBlockingDeque<Runnable>());

    @Autowired
    JdbcTemplate jdbcTemplate;

    public RowMergeResultSet calculatAllMergeResultSet(QueryUnit... quotaunits) {

        if (quotaunits == null)
            return null;

        RowMergeResultSet rowMergeResultSet = new RowMergeResultSet();
        SqlDismantling[] sqlDismantlings = rowMergeResultSet.validateQueryUnitGroup(quotaunits);

        final CountDownLatch latch = new CountDownLatch(quotaunits.length);

        for (int i = 0; i < sqlDismantlings.length; i++) {
            executorService.execute(new CalculatSqlRowTask(sqlDismantlings[i], rowMergeResultSet, latch));
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return rowMergeResultSet;

    }

    class CalculatSqlRowTask implements Runnable {

        private RowMergeResultSet rowMergeResultSet;
        private CountDownLatch latch;
        private SqlDismantling sqlDismantling;

        public CalculatSqlRowTask(SqlDismantling sqlDismantling, RowMergeResultSet rowMergeResultSet,
                CountDownLatch latch) {
            super();
            this.rowMergeResultSet = rowMergeResultSet;
            this.latch = latch;
            this.sqlDismantling = sqlDismantling;
        }

        @Override
        public void run() {
            final Dimension dimension = new Dimension(sqlDismantling.getFields());
            jdbcTemplate.query(sqlDismantling.getQueryUnit().getSql(), new ResultSetExtractor<Object>() {
                public Object extractData(ResultSet resultSet) throws SQLException, DataAccessException {

                    if (sqlDismantling.isLimitUnit() == false && rowMergeResultSet.getLimitHasFinished() == false) {
                        try {
                            synchronized (rowMergeResultSet.getLimitHasFinished()) {
                                rowMergeResultSet.getLimitHasFinished().wait();
                            }
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                    int rowNumber = 0;
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
                        rowMergeResultSet.addRowMergeResult(sqlRowResultMapping, sqlDismantling.isLimitUnit());
                        rowNumber++;
                    }

                    if (sqlDismantling.isLimitUnit()) {
                        synchronized (rowMergeResultSet.getLimitHasFinished()) {
                            rowMergeResultSet.getLimitHasFinished().notifyAll();
                            rowMergeResultSet.setLimitHasFinished(true);
                        }
                    }

                    logger.debug("{}执行结束,加载数据行是:{}", sqlDismantling.getQueryUnit().getSql(), rowNumber);
                    latch.countDown();
                    return null;
                }
            });
        }
    }

}
