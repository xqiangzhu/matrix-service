package com.cubead.matrix.providertest.base;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 随机生成测试sql工具类/避免数据换缓存影响效果
 * 
 * @author kangye
 */
public class SqlRandomGenerator {

    private static String[] fields = { "sub_tenant_id", "campaign", "adgroup", "keyword" };
    private static String tableNamePrexis = "ca_summary_136191";
    public static String[] qutas = { "cost", "pv", "uv", "impressions" };
    public final static int split_table_numbers = 10;

    public static String generteSql(final String tableName) {
        StringBuilder ab = new StringBuilder();
        ab.append("SELECT sub_tenant_id, campaign, adgroup, keyword, sum(new_visitor) cost ");
        ab.append("from ");
        ab.append(tableName);
        ab.append(" ");
        ab.append(generteWhereLogDay());
        ab.append(generteGroupSQl());
        ab.append("order by cost");
        return ab.toString();
    }

    private static StringBuilder generteGroupSQl() {

        List<String> fieldsList = new ArrayList<String>();
        for (String field : fields) {
            fieldsList.add(field);
        }

        String[] newSortFieldsStrings = new String[fields.length];

        for (int i = fields.length; i > 0; i--) {
            int index2 = new Random().nextInt(i);
            newSortFieldsStrings[fields.length - i] = fieldsList.get(index2);
            fieldsList.remove(index2);
        }

        StringBuilder ab = new StringBuilder();
        ab.append("GROUP BY ");
        for (int i = 0; i < newSortFieldsStrings.length; i++) {
            ab.append(newSortFieldsStrings[i]);
            if (i < newSortFieldsStrings.length - 1)
                ab.append(",");
            ab.append(" ");
        }

        return ab;
    }

    private static StringBuilder generteWhereLogDay() {

        int start = new Random().nextInt(10) + 1;
        int end = start + (new Random().nextInt(30) + 30);

        StringBuilder sb = new StringBuilder();

        sb.append("where log_day >= ");
        sb.append(start);
        sb.append(" AND log_day <= ");
        sb.append(end);
        sb.append(" ");

        return sb;
    }

    private static String[] generteSameQutaoTableNames(String qutao) {
        String[] tables = new String[split_table_numbers];

        for (int i = 0; i < split_table_numbers; i++) {
            tables[i] = tableNamePrexis + "_" + qutao + "_" + i;
        }

        return tables;
    }

    public static String[] generTenRandomSql(String qutao) {

        String[] sqls = new String[split_table_numbers];
        String[] tables = generteSameQutaoTableNames(qutao);

        StringBuilder ab_pre = new StringBuilder();
        ab_pre.append("SELECT sub_tenant_id, campaign, adgroup, keyword, sum(new_visitor) cost ");
        ab_pre.append("from ");

        StringBuilder ab = new StringBuilder();
        ab.append(" ");
        ab.append(generteWhereLogDay());
        ab.append(generteGroupSQl());
        ab.append(" order by cost");

        for (int i = 0; i < split_table_numbers; i++) {
            StringBuilder ab_whole = new StringBuilder();
            sqls[i] = ab_whole.append(ab_pre).append(tables[i]).append(ab).toString();
        }

        return sqls;
    }

    public static String[] generTenRandomSql() {
        String quta = qutas[new Random().nextInt(qutas.length)];
        return generTenRandomSql(quta);
    }

    public static enum TableEngine {

        InnoDB("InnoDB"), MyISAM("MyISAM");

        private String engineName;

        private TableEngine(String engineName) {
            this.engineName = engineName;
        }

        public String getTableEngine() {
            return this.engineName;
        }
    }

    public static String[] updateEnginesSql(TableEngine tableEngine) {

        String[] alterSqls = new String[split_table_numbers * qutas.length];

        for (int j = 0; j < split_table_numbers; j++) {
            for (int i = 0; i < qutas.length; i++) {
                alterSqls[i * split_table_numbers + j] = "ALTER TABLE " + tableNamePrexis + "_" + qutas[i] + "_" + j
                        + " ENGINE=" + tableEngine.getTableEngine();
            }
        }

        return alterSqls;

    }

    public static void main(String[] args) {

        for (String sql : generTenRandomSql()) {
            System.out.println(sql);
        }

        for (String sql : updateEnginesSql(TableEngine.InnoDB)) {
            System.out.println(sql);
        }
    }
}
