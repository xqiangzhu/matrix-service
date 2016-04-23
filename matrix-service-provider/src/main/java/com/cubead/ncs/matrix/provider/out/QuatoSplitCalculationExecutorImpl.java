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
import com.cubead.ncs.matrix.provider.tools.Contants;
import com.cubead.ncs.matrix.provider.tools.SqlGenerator;

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
    private static ExecutorService signleExecutorService = Executors.newSingleThreadExecutor();

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
            Boolean partitionSupport, QueryUnit... quotaunits) {

        if (partitionSupport == null)
            partitionSupport = Contants.PARTITION_PARALLEL_COMPUTING_SUPPORT;

        if (limit == null)
            limit = Contants.PAGE_LIMIT_DEFAULT;

        DubboResult<PageResult> dubboResult = new DubboResult<>();
        PageResult pageResult = new PageResult();
        dubboResult.setMessageAndStatus("查询成功", ResultStatus.SUCCESS);

        if (null == quotaunits) {
            dubboResult.setMessageAndStatus("未参入任何参数", ResultStatus.FAIL);
        }

        try {

            final String unitsHashCodeString = SqlGenerator.generterHashCode(quotaunits);
            // 缓存中查是否刚被检索过
            RowMergeResultSet rowMergeResultSet = memcachedClient.get(unitsHashCodeString);

            if (null == rowMergeResultSet) {
                // 获取结果集
                final RowMergeResultSet rowMergeResultSetNew = quatoSplitCalculationWithCountExecutor
                        .calculatAllMergeResultSet(partitionSupport, quotaunits);

                if (DubboResult.ResultStatus.FAIL.equals(rowMergeResultSetNew.getDubboResult().getResultStatus())) {
                    return rowMergeResultSetNew.getDubboResult();
                }
                // 异步缓存结果
                signleExecutorService.execute(new Runnable() {
                    public void run() {
                        try {
                            logger.info("设置{}缓存,存储{}秒", unitsHashCodeString,
                                    Contants.QUERYUNITS_RESULT_CACHE_TIME_IN_SECOND);
                            memcachedClient.set(unitsHashCodeString, Contants.QUERYUNITS_RESULT_CACHE_TIME_IN_SECOND,
                                    rowMergeResultSetNew, 1500);
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

    @Override
    public DubboResult<PageResult> calculatAllMergeResultSetAsJsonObjects(Integer page, Integer limitInteger,
            QueryUnit... quotaunits) {

        return calculatAllMergeResultSetAsJsonObjects(page, limitInteger, null, quotaunits);
    }

    @Override
    public DubboResult<PageResult> calculatAllMergeResultSetAsJsonObjects(Integer page, QueryUnit... quotaunits) {

        return calculatAllMergeResultSetAsJsonObjects(page, null, null, quotaunits);
    }

    @Override
    public DubboResult<PageResult> calculatAllMergeResultSetAsJsonObjects(Integer page, Boolean partitionSupport,
            QueryUnit... quotaunits) {
        return calculatAllMergeResultSetAsJsonObjects(page, null, partitionSupport, quotaunits);
    }

}
