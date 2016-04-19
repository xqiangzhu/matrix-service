package com.cubead.ncs.matrix.api;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * sql 解析和验证
 * 
 * @author kangye
 */
public class SqlDismantling {

    // private static final Logger logger =
    // LoggerFactory.getLogger(SqlDismantling.class);

    private QueryUnit queryUnit;
    private TreeSet<String> allFields;
    private Set<Quota> quotas;
    private Boolean isLimitUnit = false;

    public SqlDismantling(QueryUnit queryUnit) {
        this.queryUnit = queryUnit;
        validateQuotaSql();
    }

    public QueryUnit getQueryUnit() {
        return queryUnit;
    }

    public TreeSet<String> getFields() {
        return allFields;
    }

    public Set<Quota> getQuotas() {
        return quotas;
    }

    public Boolean isLimitUnit() {
        return isLimitUnit;
    }

    public void validateQuotaSql() {

        if (queryUnit == null || queryUnit.getSql() == null || queryUnit.getQuotas() == null)
            throw new IllegalArgumentException("queryUnit信息不完整,存在空值,请检查sql");

        String lowSql = queryUnit.getSql().toLowerCase();
        int startIndex = lowSql.indexOf(" select ") + 8;
        int endIndex = lowSql.indexOf(" from ");
        int limitIndex = lowSql.indexOf(" limit ");

        Set<Quota> quotasInUnit = queryUnit.getQuotas();
        String[] fieldSet = lowSql.substring(startIndex, endIndex).split(",");

        allFields = new TreeSet<String>();
        quotas = new HashSet<>();

        for (String field : fieldSet) {

            field = field.trim();

            int asIndex = field.indexOf(" as ");
            if (asIndex > -1)
                field = field.substring(asIndex + 3);

            int emptyIndex = field.indexOf(" ");
            if (emptyIndex > -1)
                field = field.substring(emptyIndex + 1).trim();

            Quota quota = Quota.getByQuota(field);
            if (quota == null || !quotasInUnit.contains(quota)) {
                allFields.add(field);
            } else {
                quotas.add(quota);
            }
        }

        if (quotas.size() < quotasInUnit.size()) {
            throw new IllegalArgumentException("queryUnit中指标不完全存在于语句中,请检查维度参数");
        }

        if (allFields.size() == 0) {
            throw new IllegalArgumentException("queryUnit不存在任何维度,请检查sql");
        }

        if (limitIndex > -1) {
            this.isLimitUnit = true;
        }

        // logger.debug("sql:{},解析的字段是:{}", queryUnit.getSql(), getFields());
        // logger.debug("sql:{},解析的维度是:{}", queryUnit.getSql(), getQuotas());
    }

    public static class QueryUnit implements Serializable {

        private static final long serialVersionUID = 6072111871217409049L;

        private String sql;
        private Set<Quota> quotas;

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public Set<Quota> getQuotas() {
            return quotas;
        }

        public void setQuotas(Set<Quota> quotas) {
            this.quotas = quotas;
        }

        public void setQuotas(Quota... quotas) {
            if (null == quotas)
                return;
            this.quotas = new HashSet<>();
            for (Quota quota : quotas) {
                this.quotas.add(quota);
            }
        }
    }

    public static void main(String[] args) {

        QueryUnit queryUnit = new QueryUnit();
        queryUnit.setSql(" select ssElect a, b e, c as c1 , fromov ,pv, cost, dfs as e fRom jkljlkj");

        Set<Quota> quotas = new HashSet<Quota>();
        quotas.add(Quota.COST);
        quotas.add(Quota.PV);
        queryUnit.setQuotas(quotas);

        System.out.println(quotas);

        SqlDismantling sqlDismantling = new SqlDismantling(queryUnit);

        System.out.println(sqlDismantling.getFields());
        System.out.println(sqlDismantling.getQuotas());

    }
}
