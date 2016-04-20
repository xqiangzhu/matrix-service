package com.cubead.matrix.providertest.base;

import org.junit.Test;
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

    @Test
    public void updateVercialTableInnoDBEngine() {
        for (String alterSql : SqlRandomGenerator.updateEnginesVercialSql(TableEngine.MyISAM)) {
            jdbcTemplate.execute(alterSql);
            logger.info("{}更新成功!", alterSql);
        }
    }
}
