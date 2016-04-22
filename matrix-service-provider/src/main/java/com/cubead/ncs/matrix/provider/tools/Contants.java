package com.cubead.ncs.matrix.provider.tools;

import java.io.InputStream;
import java.net.URLDecoder;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 常量类
 * 
 * @author kangye
 */
public final class Contants {

    public final static Properties prop = new Properties();
    private static Logger logger = LoggerFactory.getLogger(Contants.class);

    static {
        try {
            InputStream in = Contants.class.getClassLoader().getResourceAsStream(
                    URLDecoder.decode("application.properties", "UTF-8"));
            prop.load(in);
            in.close();
        } catch (Exception e) {
            logger.error("加载配置资源文件失败!{}", e.getMessage());
        }
    }

    private Contants() {

    }

    /**
     * 查询结果缓存时间
     */
    public final static int QUERYUNITS_RESULT_CACHE_TIME_IN_SECOND = Integer.parseInt(prop
            .getProperty("QUERYUNITS_RESULT_CACHE_TIME_IN_SECOND"));

    /**
     * log_day 范围起始值
     */
    public final static int LOG_DAY_START_COUNT = Integer.parseInt(prop.getProperty("LOG_DAY_START_COUNT"));

    /**
     * log_day 范围截至时间
     */
    public final static int LOG_DAY_END_COUNT = Integer.parseInt(prop.getProperty("LOG_DAY_END_COUNT"));

    public static void main(String[] args) {
        System.out.println(Contants.LOG_DAY_END_COUNT);
    }
}
