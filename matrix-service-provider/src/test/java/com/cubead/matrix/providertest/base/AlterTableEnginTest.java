package com.cubead.matrix.providertest.base;

import java.util.concurrent.ThreadFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.cubead.matrix.providertest.base.SqlRandomGenerator.TableEngine;

/**
 * 修改表引擎,慎重使用
 * 
 * @author kangye
 */
public class AlterTableEnginTest extends BaseTest {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    // @Test
    public void updateTableInnoDBEngine() {
        for (String alterSql : SqlRandomGenerator.updateEnginesSql(TableEngine.InnoDB)) {
            jdbcTemplate.execute(alterSql);
            logger.info("{}更新成功!", alterSql);
        }
    }

    // @Test
    public void updateTableMyISAMEngine() {
        for (String alterSql : SqlRandomGenerator.updateEnginesSql(TableEngine.MyISAM)) {
            jdbcTemplate.execute(alterSql);
            logger.info("{}更新成功!", alterSql);
        }
    }

    class ConcereteUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
        @Override
        public void uncaughtException(Thread thread, Throwable exception) {
            System.out.println("thread id:" + thread.getId() + " name:" + thread.getName() + " exception_message:"
                    + exception.getMessage());
        }

    }

    class ConcereteThreadFactory implements ThreadFactory {

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable);
            thread.setUncaughtExceptionHandler(new ConcereteUncaughtExceptionHandler());
            return thread;

        }

    }
}
