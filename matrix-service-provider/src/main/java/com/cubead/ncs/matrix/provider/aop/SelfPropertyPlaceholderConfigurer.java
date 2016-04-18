package com.cubead.ncs.matrix.provider.aop;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;

/**
 * 自定义加载属性---为了集群中解决activemq--clientId重复问题
 * 
 * @author kangye
 */
public class SelfPropertyPlaceholderConfigurer extends PropertyPlaceholderConfigurer {

    private static final Logger logger = LoggerFactory.getLogger(SelfPropertyPlaceholderConfigurer.class);

    @Override
    protected String convertProperty(String propertyName, String propertyValue) {
        if ("jms.clicent_id".equals(propertyName)) {
            propertyValue = propertyValue + "_" + getHostName();
            logger.debug("propertyName:{}, propertyValue: {}", propertyName, propertyValue);
        }
        return super.convertProperty(propertyName, propertyValue);
    }

    /**
     * 获得本机hostname
     * 
     * @return
     */
    private static String getHostName() {
        InetAddress netAddress;
        try {
            netAddress = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            return "";
        }

        if (null == netAddress) {
            return "";
        }
        String name = netAddress.getHostName(); // get the host address
        return name;
    }

}
