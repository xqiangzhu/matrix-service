package com.cubead.ncs.matrix.api;

import org.apache.commons.lang3.StringUtils;

/**
 * 指标
 * 
 * @author kangye
 */
public enum Quota {

    // 0 - 4 pv
    PV("pv", 0), UV("uv", 1), IMPRESSIONS("impressions", 2), COST("cost", 3), ROI("roi", 4),
    // 5 - 10
    EXT_RESOURCE_COUNT("ext_resource_count", 5);

    // 数据库映的字段名
    private String quota;

    // 序号
    private Integer serialNumber;

    private Quota(String quota, Integer serialNumber) {
        this.quota = quota;
        this.serialNumber = serialNumber;
    }

    public String getQuota() {
        return this.quota;
    }

    public Integer getSeialNumber() {
        return this.serialNumber;
    }

    public static Quota getByQuota(String quota) {

        if (StringUtils.isEmpty(quota))
            return null;

        for (Quota e : Quota.values()) {
            if (quota.equals(e.getQuota()))
                return e;
        }

        return null;
    }
}
