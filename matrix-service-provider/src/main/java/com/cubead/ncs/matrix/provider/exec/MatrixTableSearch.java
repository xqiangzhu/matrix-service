package com.cubead.ncs.matrix.provider.exec;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Component;

@SuppressWarnings("all")
@Component
public class MatrixTableSearch {

    private static final Logger logger = LoggerFactory.getLogger(MatrixTableSearch.class);

    ExecutorService executorService = Executors.newFixedThreadPool(50);

    @Autowired
    JdbcTemplate jdbcTemplate;

    public static class Dimen {

        private String field;
        private Object value;

        public void setValue(Object value) {
            this.value = value;
        }

        public String getField() {
            return field;
        }

        public Dimen(String field) {
            super();
            this.field = field;
        }

        @Override
        public String toString() {
            return "Dimen [field=" + field + ", value=" + value + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((value == null) ? 0 : value.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Dimen other = (Dimen) obj;
            if (field == null) {
                if (other.field != null)
                    return false;
            } else if (!field.equals(other.field))
                return false;
            if (value == null) {
                if (other.value != null)
                    return false;
            } else if (!value.equals(other.value))
                return false;
            return true;
        }

    }

    /**
     * 维度
     */
    public static class Dimension implements Cloneable {

        private List<Dimen> dimens;

        public Dimension(String... fields) {

            dimens = new ArrayList<>();
            if (null == fields)
                return;
            for (String dimen : fields) {
                dimens.add(new Dimen(dimen));
            }
        }

        public String parseAsKey() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < dimens.size(); i++) {
                sb.append(dimens.get(i).value);
                sb.append("-");
            }
            return sb.toString();
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((dimens == null) ? 0 : dimens.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Dimension other = (Dimension) obj;
            if (dimens == null) {
                if (other.dimens != null)
                    return false;
            } else if (!dimens.equals(other.dimens))
                return false;
            return true;
        }

        public List<Dimen> getDimens() {
            return dimens;
        }

        public void setDimens(List<Dimen> dimens) {
            this.dimens = dimens;
        }
    }

    /**
     * 指标
     */
    public enum Quota {
        PV(0), UV(1), IMPRESSIONS(2), COST(3), OTHER(4);

        private int index;

        private Quota(int index) {
            this.index = index;
        }

        public int getIndex() {
            return index;
        }
    }

    public static class QuotaField {

        private Quota quota;

        private Double value;

        private String paserKeys;

        public String getPaserKeys() {
            return paserKeys;
        }

        public void setPaserKeys(String paserKeys) {
            this.paserKeys = paserKeys;
        }

        public Quota getQuota() {
            return quota;
        }

        public Double getValue() {
            return value;
        }

        public QuotaField(Quota quota, Double value) {
            super();
            this.quota = quota;
            this.value = value;
        }

        @Override
        public String toString() {
            return "QuotaField [quota=" + quota + ", value=" + value + ", paserKeys=" + paserKeys + "]";
        }
    }

    interface QueryCallBack {
        public void orderFieldHandle(QuotaField quotaField);
    }

    abstract class QuotaCalculationTask implements Callable<List<QuotaField>> {

        private String SQL;
        private Quota quota;
        @SuppressWarnings("unused")
        private Dimension dimension;
        private volatile boolean isOrderQuota = false;
        private QueryCallBack queryCallBack;

        abstract String setValueField();

        protected void setOrderQuota() {
            isOrderQuota = true;
        }

        public QuotaCalculationTask(String SQL, Quota quota, Dimension dimension) {
            this.SQL = SQL;
            this.quota = quota;
            this.dimension = dimension;
        }

        public QuotaCalculationTask(String SQL, Quota quota, Dimension dimension, QueryCallBack queryCallBack) {
            this.SQL = SQL;
            this.quota = quota;
            this.dimension = dimension;
            this.queryCallBack = queryCallBack;
        }

        // 计算某个维度查询实现
        @Override
        public List<QuotaField> call() throws Exception {

            CompletionService<List<QuotaField>> completionService = new ExecutorCompletionService<List<QuotaField>>(
                    executorService);

            final String[] tableNames = { "ca_summary_136191_uv", "ca_summary_136191_impressions",
                    "ca_summary_136191_pv", "ca_summary_136191_cost" };

            for (int i = 0; i < 10; i++) {
                final int index = i;
                completionService.submit(new Callable<List<QuotaField>>() {
                    public List<QuotaField> call() throws Exception {

                        String activSql = SQL;
                        for (final String tableName : tableNames) {
                            activSql = activSql.toLowerCase().replaceAll(tableName, tableName + "_" + index);
                        }
                        // logger.info(activSql);
                        return jdbcTemplate.query(activSql, new ResultSetExtractor<List<QuotaField>>() {
                            public List<QuotaField> extractData(ResultSet resultSet) throws SQLException,
                                    DataAccessException {

                                List<QuotaField> quotaFields = new ArrayList<>();
                                while (resultSet.next()) {

                                    Dimension dimensionTemp = new Dimension();
                                    List<Dimen> dimens = new ArrayList<>();
                                    for (int i = 0; i < dimension.getDimens().size(); i++) {

                                        String field = dimension.getDimens().get(i).field;
                                        Dimen dimen = new Dimen(field);
                                        dimen.setValue(resultSet.getObject(field));
                                        dimens.add(dimen);

                                    }
                                    dimensionTemp.setDimens(dimens);

                                    QuotaField quotaField = new QuotaField(quota, resultSet.getDouble(setValueField()));
                                    quotaField.setPaserKeys(dimensionTemp.parseAsKey());
                                    if (null != queryCallBack) {
                                        queryCallBack.orderFieldHandle(quotaField);
                                    }
                                    quotaFields.add(quotaField);
                                }
                                return quotaFields;
                            }
                        });

                    }
                });
            }

            List<QuotaField> allQuotaFields = new ArrayList<>();

            for (int i = 0; i < 10; i++) {
                List<QuotaField> quotaFields = completionService.take().get();
                // logger.info("子表查询结果：{}", quotaFields);
                allQuotaFields.addAll(quotaFields);
            }

            // logger.info("quota: {},size: {}", quota, allQuotaFields.size());
            return allQuotaFields;
        }

        public Dimension getDimension() {
            return dimension;
        }
    }

    public List<List<QuotaField>> getExampleStatistics(final TreeSet<Integer> fieldHashSet) {

        List<List<QuotaField>> quotaResults = new ArrayList<>();
        CompletionService<List<QuotaField>> completionService = new ExecutorCompletionService<List<QuotaField>>(
                executorService);

        // 计算维度 cost
        completionService.submit(new QuotaCalculationTask(COST_SQL, Quota.COST, new Dimension(cost_sql_fields)) {
            String setValueField() {
                return "cost";
            }
        });
        // 计算维度impression
        completionService.submit(new QuotaCalculationTask(IMPRESSIONS_SQL, Quota.IMPRESSIONS, new Dimension(
                cost_sql_fields)) {
            String setValueField() {
                return "impressions";
            }
        });
        // 计算维度pv
        completionService.submit(new QuotaCalculationTask(PV_SQL, Quota.PV, new Dimension(cost_sql_fields)) {
            String setValueField() {
                return "pv";
            }
        });
        // 计算维度uv
        completionService.submit(new QuotaCalculationTask(UV_SQL, Quota.UV, new Dimension(cost_sql_fields)) {
            String setValueField() {
                return "uv";
            }
        });
        // 计算维度
        completionService.submit(new QuotaCalculationTask(UV_SQL, Quota.OTHER, new Dimension(cost_sql_fields)) {
            String setValueField() {
                return "uv";
            }
        });

        for (int i = 0; i < 5; i++) {

            try {

                List<QuotaField> quotaFields = completionService.take().get();
                quotaResults.add(quotaFields);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        executorService.shutdown();

        return quotaResults;
    }

    /* 多日各关键词消费合计 */
    String[] cost_sql_fields = { "sub_tenant_id", "campaign", "adgroup", "keyword" };
    public static final String COST_SQL = "SELECT sub_tenant_id, campaign, adgroup, keyword, sum(cost) cost FROM ca_summary_136191_cost "
            + "WHERE log_day >=4 AND log_day <= 50 GROUP BY campaign,adgroup,keyword,sub_tenant_id order by cost";

    /* 多日各关键词展示量合计 */
    public static final String IMPRESSIONS_SQL = "SELECT sub_tenant_id, campaign, adgroup, keyword, sum(impressions) impressions FROM ca_summary_136191_impressions"
            + " WHERE log_day >=5 AND log_day <= 150 GROUP BY keyword,adgroup,campaign,sub_tenant_id";

    /* 每日各关键词合计pv */
    public static final String PV_SQL = "SELECT sub_tenant_id, campaign, adgroup, keyword, COUNT(*) pv  FROM ca_summary_136191_pv "
            + "WHERE log_day >=5 AND log_day <= 150 GROUP BY adgroup,campaign,sub_tenant_id, keyword";

    /* 每日各关键词合计uv */
    public static final String UV_SQL = " SELECT sub_tenant_id, campaign, adgroup, keyword, COUNT(distinct own_uid) as uv "
            + "FROM ca_summary_136191_uv WHERE log_day >=5 AND log_day <= 150 GROUP BY adgroup,campaign,sub_tenant_id, keyword";

}
