package com.cubead.matrix.providertest.base;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.cubead.ncs.matrix.provider.tools.Contants;

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

    public static String[] updateEnginesVercialSql(TableEngine tableEngine) {

        String[] alterSqls = new String[qutas_verticals.length];

        for (int j = 0; j < qutas_verticals.length; j++) {
            alterSqls[j] = "ALTER TABLE " + tableNamePrexis + "_" + qutas_verticals[j] + " ENGINE="
                    + tableEngine.getTableEngine();
        }

        return alterSqls;

    }

    public final static String[] parations = { "p1_", "p2_", "p3_", "p4_", "p5_", "p6_", "p7_", "p8_", "p9_", "p10_",
            "p11_", "p12_" };

    public static String[] genertePartitionSqls(int start, int end) {

        if (end > Contants.LOG_DAY_END_COUNT)
            end = Contants.LOG_DAY_END_COUNT;
        if (start < Contants.LOG_DAY_START_COUNT)
            start = Contants.LOG_DAY_START_COUNT;

        int partitionLength = (end - start) / 30;
        int currentPartition = (start - Contants.LOG_DAY_START_COUNT) / 30;

        String[] sqls = new String[partitionLength];

        StringBuilder groupSql = SqlRandomGenerator.generteGroupSQl();
        for (int i = 0; i < partitionLength; i++) {

            int temp_partition = currentPartition + i;
            int temp_start = temp_partition * 30 + Contants.LOG_DAY_START_COUNT;
            int temp_end = temp_start + 30;

            if (i == 0)
                temp_start = start;

            if (temp_start < start)
                temp_start = start;

            if (temp_end > end)
                temp_end = end;

            StringBuilder logBuilder = new StringBuilder();
            if (temp_end - temp_start == 30) {
                logBuilder.append("PARTITION (").append(parations[temp_partition]).append(") ");
            } else {
                logBuilder.append(" where log_day > ").append(temp_start - 1).append(" and log_day < ")
                        .append(temp_end).append(" ");
            }

            StringBuilder spBuilder = new StringBuilder();
            spBuilder.append("SELECT sub_tenant_id, campaign, adgroup, keyword, sum(costs_per_click) roi ")
                    .append(" from ca_summary_136191_compressed_1yr ").append(logBuilder).append(groupSql).append("  ");

            sqls[i] = spBuilder.toString();
        }

        return sqls;
    }

    public static void main(String[] args) {

        for (String sql : generTenRandomSql()) {
            System.out.println(sql);
        }

        for (String sql : updateEnginesSql(TableEngine.InnoDB)) {
            System.out.println(sql);
        }

        for (String s : genertePartitionSqls(100, 500)) {
            System.out.println(s);
        }

    }
}
