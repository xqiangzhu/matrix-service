package com.cubead.matrix.providertest.base;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * 测试模板
 * 
 * @author kangye
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({ "classpath*:spring-main-test.xml" })
public class BaseTest {

    protected final static Logger logger = LoggerFactory.getLogger(BaseTest.class);
    protected static long current_time = System.currentTimeMillis();

    @BeforeClass
    public static void beforetest() {
        logger.warn("======================单元测试开始=================");
    }

    @AfterClass
    public static void aftertest() {
        logger.warn("======================单元测试结束=================");
    }

    @Before
    public void methodBeginTime() {
        current_time = System.currentTimeMillis();
    }

    @After
    public void methodFinishedTime() {
        logger.warn("-------------------------------当前方法运行耗时:{}秒------------------------",
                (System.currentTimeMillis() - current_time) / (double) 1000.0);
    }
}
