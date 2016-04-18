package com.ishidai.dubbo.consumer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({ "classpath*:META-INF/spring/*.xml" })
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

}
