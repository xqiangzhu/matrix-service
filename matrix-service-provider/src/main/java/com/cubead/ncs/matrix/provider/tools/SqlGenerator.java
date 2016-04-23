package com.cubead.ncs.matrix.provider.tools;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.cubead.ncs.matrix.api.SqlDismantling.QueryUnit;

/**
 * 随机生成测试sql工具类/避免数据换缓存影响效果
 * 
 * @author kangye
 */
public class SqlGenerator {

    protected static String[] fields = { "sub_tenant_id", "campaign", "adgroup", "keyword" };
    protected static String tableNamePrexis = "ca_summary_136191";
    public static String[] qutas = { "cost", "pv", "uv", "impressions" };
    public final static int split_table_numbers = 10;
    public static String[] qutas_verticals = { "compressed", "pv", "uv", "roi" };

    public static String[] generteVerticalsSql() {

        String[] sqls = new String[qutas_verticals.length];
        String[] tables = generteSameQutaoVerticalsTableNames();

        StringBuilder ab_pre = new StringBuilder();
        ab_pre.append("SELECT sub_tenant_id, campaign, adgroup, keyword, sum(new_visitor) cost ");
        ab_pre.append("from ");

        for (int i = 0; i < split_table_numbers; i++) {
            StringBuilder ab_whole = new StringBuilder();
            ab_whole.append(ab_pre).append(tables[i]).append(generteWhereLogDay()).append(generteGroupSQl());
            sqls[i] = ab_whole.toString();
        }

        return sqls;

    }

    private static String[] generteSameQutaoVerticalsTableNames() {
        String[] tables = new String[qutas_verticals.length];

        for (int i = 0; i < qutas_verticals.length; i++) {
            tables[i] = tableNamePrexis + "_" + qutas_verticals[i];
        }

        return tables;
    }

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

    public static StringBuilder generteGroupSQl() {

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

    public static StringBuilder generteWhereLogDay() {
        return generteWhereLogDay(1, 1);
    }

    public static StringBuilder generteWhereLogDay(int startPartition, int partitionLength) {

        int start = new Random().nextInt(10) + (startPartition - 1) * 30 + Contants.LOG_DAY_START_COUNT;
        int end = start + (partitionLength - 1) * 30 + new Random().nextInt(20);

        if (end > Contants.LOG_DAY_END_COUNT)
            end = Contants.LOG_DAY_END_COUNT;

        StringBuilder sb = new StringBuilder();

        sb.append("where log_day > ");
        sb.append(start);
        sb.append(" AND log_day < ");
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

    public static void main(String[] args) {

        for (String sql : generTenRandomSql()) {
            System.out.println(sql);
        }
    }
}
