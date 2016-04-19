package com.cubead.ncs.matrix.api;

import java.util.List;

import com.alibaba.fastjson.JSONObject;

/**
 * 分页结果
 * 
 * @author kangye
 */
public class PageResult {

    // 分页的结果
    private List<JSONObject> pageResult;
    // 总量
    private Integer count;

    public List<JSONObject> getPageResult() {
        return pageResult;
    }

    public void setPageResult(List<JSONObject> pageResult) {
        this.pageResult = pageResult;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "PageResult [count=" + count + ", pageResult=" + pageResult + " ]";
    }
}
