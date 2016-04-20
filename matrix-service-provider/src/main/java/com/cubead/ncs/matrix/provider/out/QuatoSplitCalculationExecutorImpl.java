package com.cubead.ncs.matrix.provider.out;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.rubyeye.xmemcached.MemcachedClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSONObject;
import com.cubead.ncs.matrix.api.DubboResult;
import com.cubead.ncs.matrix.api.DubboResult.ResultStatus;
import com.cubead.ncs.matrix.api.PageResult;
import com.cubead.ncs.matrix.api.QuatoSplitCalculationExecutorInf;
import com.cubead.ncs.matrix.api.SqlDismantling.QueryUnit;
import com.cubead.ncs.matrix.provider.compress.RowMergeResultTransform;
import com.cubead.ncs.matrix.provider.exec.QuatoSplitCalculationExecutor;
import com.cubead.ncs.matrix.provider.exec.QuatoSplitCalculationWithCountExecutor;
import com.cubead.ncs.matrix.provider.exec.RowMergeResultSet;

@Component
public class QuatoSplitCalculationExecutorImpl implements QuatoSplitCalculationExecutorInf {

    private static final Logger logger = LoggerFactory.getLogger(QuatoSplitCalculationExecutorImpl.class);

    @Autowired
    private QuatoSplitCalculationExecutor quatoSplitCalculationExecutor;

    @Autowired
    private QuatoSplitCalculationWithCountExecutor quatoSplitCalculationWithCountExecutor;

    @Autowired
    private RowMergeResultTransform resultTransform;

    @Autowired
    private MemcachedClient memcachedClient;

    // 线程是尽量小,减少和DB查询线程抢占CPU
    private static ExecutorService executorService = Executors.newFixedThreadPool(1);

    @Override
    public List<JSONObject> calculatLimitMergeResultSetAsJsonObjects(QueryUnit... quotaunits) {

        if (null == quotaunits)
            throw new IllegalArgumentException("未参入任何参数");

        // 获取结果集
        RowMergeResultSet rowMergeResultSet = quatoSplitCalculationExecutor.calculatAllMergeResultSet(quotaunits);
        logger.debug("获取结果集:{}", rowMergeResultSet.getRowQuotaSetMap().size());

        // 将结果转化为JSON串数组
        List<JSONObject> josnRows = resultTransform.transFormRowResultSetAsAJsonObjects(rowMergeResultSet);
        logger.debug("将结果转化为JSON串数组:{}", josnRows.size());

        return josnRows;
    }

    @Override
    public DubboResult<PageResult> calculatAllMergeResultSetAsJsonObjects(Integer page, Integer limit,
            QueryUnit... quotaunits) {

        DubboResult<PageResult> dubboResult = new DubboResult<>();
        PageResult pageResult = new PageResult();
        dubboResult.setMessageAndStatus("查询成功", ResultStatus.SUCCESS);

        if (null == quotaunits) {
            dubboResult.setMessageAndStatus("未参入任何参数", ResultStatus.FAIL);
        }

        try {

            final String unitsHashCodeString = generterHashCode(quotaunits);
            // 缓存中查是否刚被检索过
            RowMergeResultSet rowMergeResultSet = memcachedClient.get(unitsHashCodeString);

            if (null == rowMergeResultSet) {
                // 获取结果集
                final RowMergeResultSet rowMergeResultSetNew = quatoSplitCalculationWithCountExecutor
                        .calculatAllMergeResultSet(quotaunits);

                if (DubboResult.ResultStatus.FAIL.equals(rowMergeResultSetNew.getDubboResult().getResultStatus())) {
                    return rowMergeResultSetNew.getDubboResult();
                }
                // 异步保存结果
                executorService.execute(new Runnable() {
                    public void run() {
                        try {
                            logger.info("设置{}缓存,存储30秒", unitsHashCodeString);
                            memcachedClient.set(unitsHashCodeString, 30, rowMergeResultSetNew, 1500);
                        } catch (Exception e) {
                            logger.warn("{}未正确的缓存结果", unitsHashCodeString);
                        }
                    }
                });

                rowMergeResultSet = rowMergeResultSetNew;
            } else {
                logger.info("key:{}缓存命中", unitsHashCodeString);
            }

            pageResult.setCount(rowMergeResultSet.getRowQuotaSetMap().size());

            // 将结果转化为JSON串数组
            List<JSONObject> josnRows = resultTransform.transFormRowResultSetAsAJsonObjects((page - 1) * limit, page
                    * limit - 1, rowMergeResultSet);

            pageResult.setPageResult(josnRows);
            dubboResult.setBean(pageResult);

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("查询异常:{}", e.getMessage());
            dubboResult.setMessageAndStatus(e.getMessage(), ResultStatus.FAIL);
        }

        return dubboResult;
    }

    /**
     * 根据query组合计算其hash串,区别同一个查询
     * 
     * @author kangye
     * @param quotaunits
     * @return
     */
    public static String generterHashCode(QueryUnit... quotaunits) {

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < quotaunits.length; i++) {
            sb.append(quotaunits[i].hashCode());
            if (i < quotaunits.length) {
                sb.append("-");
            }
        }
        return sb.toString();
    }
}
