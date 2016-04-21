package com.cubead.matrix.providertest.base;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

public class PartitionMutilThreadQueryTest extends BaseTest {

    @Autowired
    JdbcTemplate jdbcTemplate;

    private static ExecutorService executorService = new ThreadPoolExecutor(10, 30, 10, TimeUnit.SECONDS,
            new LinkedBlockingDeque<Runnable>());

    // 按每个区多线程并行执行
    @Test
    public void queryTimeCost() {
        logger.info("-------------------------------按每个区多线程并行执行------------------------");
        String[] sqls = SqlRandomGenerator.genertePartitionSqls(12, 500);
        final CountDownLatch latch = new CountDownLatch(sqls.length);

        for (final String sql : sqls) {
            executorService.execute(new Runnable() {
                public void run() {
                    long time = System.currentTimeMillis();
                    try {
                        jdbcTemplate.query(sql, new ResultSetExtractor<Object>() {
                            public Object extractData(ResultSet arg0) throws SQLException, DataAccessException {

                                return null;
                            }
                        });
                    } catch (DataAccessException e) {
                        e.printStackTrace();
                    } finally {
                        logger.info("{}执行结束，耗时:{}ms", sql, System.currentTimeMillis() - time);
                        latch.countDown();
                    }
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        executorService.shutdown();

    }

    // 按每个区分区执行
    @Test
    public void randomExecuOnce() {
        logger.info("-------------------------------按每个区分区执行------------------------");
        String[] sqls = SqlRandomGenerator.genertePartitionSqls(12, 500);
        for (String sql : sqls) {
            long time = System.currentTimeMillis();
            jdbcTemplate.query(sql, new ResultSetExtractor<Object>() {
                public Object extractData(ResultSet arg0) throws SQLException, DataAccessException {

                    return null;
                }
            });
            logger.info("{}执行结束，耗时:{}ms", sql, System.currentTimeMillis() - time);
        }
    }

    // 在大表上执行一个全区查询
    @Test
    public void exeInWholeSql() {
        logger.info("-------------------------------在大表上执行一个全区查询------------------------");
        String sql = "SELECT sub_tenant_id, campaign, adgroup, keyword, sum(costs_per_click) roi  from ca_summary_136191_compressed_1yr  where log_day > 30 and log_day < 418 GROUP BY campaign, keyword, sub_tenant_id, adgroup";
        long time = System.currentTimeMillis();
        jdbcTemplate.query(sql, new ResultSetExtractor<Object>() {
            public Object extractData(ResultSet arg0) throws SQLException, DataAccessException {

                return null;
            }
        });
        logger.info("{}执行结束，耗时:{}ms", sql, System.currentTimeMillis() - time);

    }
}
