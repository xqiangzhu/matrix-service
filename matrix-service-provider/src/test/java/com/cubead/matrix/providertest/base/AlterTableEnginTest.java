package com.cubead.matrix.providertest.base;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.cubead.ncs.matrix.provider.tools.EngineUpdateSqlGenerator;
import com.cubead.ncs.matrix.provider.tools.EngineUpdateSqlGenerator.TableEngine;

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
        for (String alterSql : EngineUpdateSqlGenerator.updateEnginesSql(TableEngine.InnoDB)) {
            jdbcTemplate.execute(alterSql);
            logger.info("{}更新成功!", alterSql);
        }
    }

    // @Test
    public void updateTableMyISAMEngine() {
        for (String alterSql : EngineUpdateSqlGenerator.updateEnginesSql(TableEngine.MyISAM)) {
            jdbcTemplate.execute(alterSql);
            logger.info("{}更新成功!", alterSql);
        }
    }

    @Test
    public void updateVercialTableInnoDBEngine() {
        for (String alterSql : EngineUpdateSqlGenerator.updateEnginesVercialSql(TableEngine.MyISAM)) {
            jdbcTemplate.execute(alterSql);
            logger.info("{}更新成功!", alterSql);
        }
    }
}
