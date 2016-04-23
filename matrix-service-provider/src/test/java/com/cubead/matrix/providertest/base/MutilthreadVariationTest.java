package com.cubead.matrix.providertest.base;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

import com.cubead.ncs.matrix.provider.exec.SqlGenerator;
import com.cubead.ncs.matrix.provider.exec.MatrixTableSearch.Dimen;
import com.cubead.ncs.matrix.provider.exec.MatrixTableSearch.Dimension;
import com.cubead.ncs.matrix.provider.exec.MatrixTableSearch.Quota;
import com.cubead.ncs.matrix.provider.exec.MatrixTableSearch.QuotaField;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * 多线程查询的多种实现对比
 * 
 * @author kangye
 */
public class MutilthreadVariationTest extends BaseTest {

    static ExecutorService executorService = Executors.newFixedThreadPool(9);
    static ExecutorService singleExecutorService = Executors.newSingleThreadExecutor();

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    public void sqlExcuteTimeMilis() {
        long t1 = System.currentTimeMillis();
        final Dimension dimension = new Dimension("sub_tenant_id", "campaign", "adgroup", "keyword");
        List<QuotaField> quotaFields = jdbcTemplate.query(SqlGenerator.generteSql("ca_summary_136191_pv_1"),
                new ResultSetExtractor<List<QuotaField>>() {
                    public List<QuotaField> extractData(ResultSet resultSet) throws SQLException, DataAccessException {
                        List<QuotaField> quotaFields = new ArrayList<>();

                        return quotaFields;
                    }
                });
        logger.info("单线程执行一个sql查询耗时:{}", (System.currentTimeMillis() - t1) + " ms");
    }

    @Test
    public void sqlExcuteTimeMilisWithTenSql() {
        long t1 = System.currentTimeMillis();
        for (final String sql : SqlGenerator.generTenRandomSql()) {
            logger.info(sql);
            jdbcTemplate.query(sql, new ResultSetExtractor<List<QuotaField>>() {
                public List<QuotaField> extractData(ResultSet resultSet) throws SQLException, DataAccessException {
                    List<QuotaField> quotaFields = new ArrayList<>();

                    return quotaFields;
                }
            });
        }

        logger.info("10查询串行执行:{}", (System.currentTimeMillis() - t1) + " ms");
    }

    @Test
    public void sqlExecuteMutilThreadTimeTest() {

        CompletionService<List<QuotaField>> completionService = new ExecutorCompletionService<List<QuotaField>>(
                executorService);
        long t1 = System.currentTimeMillis();

        for (final String sql : SqlGenerator.generTenRandomSql()) {
            logger.info(sql);
            completionService.submit(new Callable<List<QuotaField>>() {
                public List<QuotaField> call() throws Exception {
                    return jdbcTemplate.query(sql, new ResultSetExtractor<List<QuotaField>>() {
                        public List<QuotaField> extractData(final ResultSet resultSet) throws SQLException,
                                DataAccessException {

                            List<QuotaField> quotaFields = new ArrayList<>();

                            return quotaFields;
                        }
                    });
                }
            });
        }

        List<QuotaField> allQuotaFields = new ArrayList<>();

        for (int i = 0; i < SqlGenerator.split_table_numbers; i++) {
            try {
                List<QuotaField> quotaFields = completionService.take().get();
                // logger.info("子表查询结果：{}", quotaFields);
                allQuotaFields.addAll(quotaFields);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        logger.info("ten sql查询耗时:{}", (System.currentTimeMillis() - t1) + " ms");
    }

    @AfterClass
    public static void afterClass() {
        // RpcContext.getContext().getFuture();
        logger.info("close the pool");
        executorService.shutdown();
        singleExecutorService.shutdown();
    }

    @Test
    public void sqlExecuteMutilThreadTimeWithAsyBlockingQueen() {

        long t1 = System.currentTimeMillis();
        final BlockingQueue<ResultSet> resultSets = new ArrayBlockingQueue<ResultSet>(10);

        for (final String sql : SqlGenerator.generTenRandomSql()) {
            logger.info(sql);
            executorService.execute(new Runnable() {
                public void run() {
                    jdbcTemplate.query(sql, new ResultSetExtractor<Object>() {
                        public Object extractData(final ResultSet resultSet) throws SQLException, DataAccessException {
                            singleExecutorService.execute(new Runnable() {
                                public void run() {
                                    try {
                                        resultSets.put(resultSet);
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                }
                            });
                            return null;
                        }

                    });
                }
            });
        }

        List<ResultSet> allQuotaFields = new ArrayList<>();

        for (int i = 0; i < SqlGenerator.split_table_numbers; i++) {
            try {
                ResultSet quotaFields = resultSets.take();
                // logger.info("子表查询结果：{}", quotaFields);
                allQuotaFields.add(quotaFields);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        logger.info("sqlExecuteMutilThreadTimeWithAsyBlockingQueen ten sql查询耗时:{}", (System.currentTimeMillis() - t1)
                + " ms");
    }

    // @Test
    public void sqlExecuteMutilThreadTimeWithAsyDistruptor() {

        final long t1 = System.currentTimeMillis();

        ExecutorService exec = Executors.newCachedThreadPool();
        Disruptor<QutotaEvent> disruptor = new Disruptor<QutotaEvent>(QutotaEvent.EVENT_FACTORY, 1024, exec);

        final AtomicInteger index = new AtomicInteger(0);
        final AtomicInteger indexInEvent = new AtomicInteger(0);

        final EventHandler<QutotaEvent> handler = new EventHandler<QutotaEvent>() {
            public void onEvent(final QutotaEvent event, final long sequence, final boolean endOfBatch)
                    throws Exception {
                // System.out.println("Sequence: " + sequence);
                // System.out.println("ValueEvent: " + event.getValue().size());
                if (indexInEvent.incrementAndGet() == SqlGenerator.split_table_numbers) {
                    logger.info("sqlExecuteMutilThreadTimeWithAsyBlockingQueen ten sql查询耗时:{}",
                            (System.currentTimeMillis() - t1) + " ms");
                }
            }
        };
        // Build dependency graph
        disruptor.handleEventsWith(handler);
        final RingBuffer<QutotaEvent> ringBuffer = disruptor.start();
        final Dimension dimension = new Dimension("sub_tenant_id", "campaign", "adgroup", "keyword");

        for (final String sql : SqlGenerator.generTenRandomSql()) {
            logger.info(sql);
            executorService.execute(new Runnable() {
                public void run() {
                    jdbcTemplate.query(sql, new ResultSetExtractor<Object>() {
                        public Object extractData(final ResultSet resultSet) throws SQLException, DataAccessException {

                            int j = index.incrementAndGet();
                            long t2 = System.currentTimeMillis();
                            List<QuotaField> quotaFields = new ArrayList<>();
                            while (resultSet.next()) {
                                // time cost so mush*** TODO
                                QuotaField quotaField = new QuotaField(Quota.PV, resultSet.getDouble("cost"));
                                quotaField.setPaserKeys("");
                                quotaFields.add(quotaField);
                            }
                            logger.info("{}::{}", j, (System.currentTimeMillis() - t2) + " ms");
                            long seq = ringBuffer.next();
                            QutotaEvent valueEvent = ringBuffer.get(seq);
                            valueEvent.setValue(quotaFields);
                            ringBuffer.publish(seq);

                            return null;
                        }

                    });
                }
            });
        }

        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        disruptor.shutdown();
        exec.shutdown();

    }

    // @Test
    public void sqlExecuteMutilThreadTimeTestForCountDownLunch() {

        final CountDownLatch latch = new CountDownLatch(SqlGenerator.split_table_numbers);
        long t1 = System.currentTimeMillis();
        List<QuotaField> allQuotaFields = new ArrayList<>();
        final Dimension dimension = new Dimension("sub_tenant_id", "campaign", "adgroup", "keyword");

        for (final String sql : SqlGenerator.generTenRandomSql()) {
            executorService.execute(new Runnable() {

                @Override
                public void run() {
                    List<QuotaField> quotaFields = jdbcTemplate.query(sql, new ResultSetExtractor<List<QuotaField>>() {
                        public List<QuotaField> extractData(ResultSet resultSet) throws SQLException,
                                DataAccessException {

                            List<QuotaField> quotaFields = new ArrayList<>();
                            while (resultSet.next()) {

                                Dimension dimensionTemp = new Dimension();
                                List<Dimen> dimens = new ArrayList<>();
                                for (int i = 0; i < dimension.getDimens().size(); i++) {

                                    String field = dimension.getDimens().get(i).getField();
                                    Dimen dimen = new Dimen(field);
                                    dimen.setValue(resultSet.getObject(field));
                                    dimens.add(dimen);

                                }
                                dimensionTemp.setDimens(dimens);

                                QuotaField quotaField = new QuotaField(Quota.PV, resultSet.getDouble("cost"));
                                quotaField.setPaserKeys(dimensionTemp.parseAsKey());
                                quotaFields.add(quotaField);
                            }
                            return quotaFields;
                        }
                    });

                    latch.countDown();
                    // bl.add(quotaFields);
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }

        for (int i = 0; i < SqlGenerator.split_table_numbers; i++) {
            try {
                // List<QuotaField> quotaFields = bl.take();
                // logger.info("子表查询结果：{}", quotaFields);
                // allQuotaFields.addAll(quotaFields);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        logger.info(" ten sql with dimen package 查询耗时:{}", (System.currentTimeMillis() - t1) + " ms");
    }
}
