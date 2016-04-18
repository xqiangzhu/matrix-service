package com.cubead.matrix.providertest.base;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
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
@ContextConfiguration({ "classpath*:m-dubbo/spring-main.xml" })
public class BaseTest {

    protected final static Logger logger = LoggerFactory.getLogger(BaseTest.class);

    @BeforeClass
    public static void beforetest() {
        logger.warn("======================单元测试开始=================");
    }

    @AfterClass
    public static void aftertest() {
        logger.warn("======================单元测试结束=================");
    }

    @Test
    public void testInit() {

    }
}
