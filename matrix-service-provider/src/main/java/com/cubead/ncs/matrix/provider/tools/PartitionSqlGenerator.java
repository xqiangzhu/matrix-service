package com.cubead.ncs.matrix.provider.tools;

/**
 * 分区sql语句生产器
 * 
 * @author kangye
 */
public final class PartitionSqlGenerator {

    public final static String[] parations = { "p1_", "p2_", "p3_", "p4_", "p5_", "p6_", "p7_", "p8_", "p9_", "p10_",
            "p11_", "p12_" };

    public static String[] genertePartitionSqls(int start, int end) {
        String fromPart = "SELECT sub_tenant_id, campaign, adgroup, keyword, sum(costs_per_click) roi from ca_summary_136191_compressed_1yr";
        return genertePartitionSqls(start, end, fromPart, SqlGenerator.generteGroupSQl().toString());
    }

    public static String[] genertePartitionSqls(int start, int end, String selectPart, String groupPart) {

        if (end > Contants.LOG_DAY_END_COUNT)
            end = Contants.LOG_DAY_END_COUNT;
        if (start < Contants.LOG_DAY_START_COUNT)
            start = Contants.LOG_DAY_START_COUNT;

        int partitionLength = (end - start) / 30;
        int currentPartition = (start - Contants.LOG_DAY_START_COUNT) / 30;

        String[] sqls = new String[partitionLength + 1];

        for (int i = 0; i < partitionLength + 1; i++) {

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
                logBuilder.append(" PARTITION (").append(parations[temp_partition]).append(") ");
            } else {
                logBuilder.append(" where log_day > ").append(temp_start - 1).append(" and log_day < ")
                        .append(temp_end).append(" ");
            }

            StringBuilder spBuilder = new StringBuilder();
            spBuilder.append(selectPart).append(logBuilder).append(groupPart);

            sqls[i] = spBuilder.toString();
        }

        return sqls;
    }

    public static String[] generatPartitionSql(String sql) {

        sql = sql.toLowerCase().replaceAll("\\s{1,}", " ");
        String selectPart = sql.substring(sql.indexOf(" select ") + 1, sql.indexOf(" where "));
        String logDayPart = sql.substring(sql.indexOf(" where ") + 1, sql.indexOf(" group "));
        String groupPart = sql.substring(sql.indexOf(" group ") + 1);

        int startDay = -1;
        int endDay = -1;
        try {
            String[] logDays = logDayPart.split(" and ");
            for (String s : logDays) {
                if (s.contains("log_day > ")) {
                    String parStr = s.replaceAll(">|log_day|where", "").trim();
                    startDay = Integer.parseInt(parStr);
                }
                if (s.contains("log_day < ")) {
                    String parStr = s.replaceAll("<|log_day|where", "").trim();
                    endDay = Integer.parseInt(parStr);
                }
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("log_day 参数不是整形");
        }
        if (startDay * endDay < -1) {
            throw new IllegalArgumentException("log_day 参数值大小设置错误");
        }

        return genertePartitionSqls(startDay, endDay, selectPart, groupPart);
    }

    public static void main(String[] args) {

        for (String s : genertePartitionSqls(99, 419)) {
            System.out.println(s);
        }

        String sql = "SELECT sub_tenant_id, campaign, adgroup, keyword, sum(costs_per_click) roi  from ca_summary_136191_compressed_1yr  "
                + "where log_day > 99 and log_day < 119 and url ='' GROUP BY sub_tenant_id, campaign, keyword, adgroup  ";

        for (String s : generatPartitionSql(sql)) {
            System.out.println(s);
        }

    }
}
