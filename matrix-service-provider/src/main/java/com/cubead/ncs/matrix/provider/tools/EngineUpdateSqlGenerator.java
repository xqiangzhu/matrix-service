package com.cubead.ncs.matrix.provider.tools;

/**
 * 批量引擎修改sql
 * 
 * @author kangye
 */
public class EngineUpdateSqlGenerator extends SqlGenerator {

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

    public static void main(String[] args) {

        for (String sql : updateEnginesSql(TableEngine.InnoDB)) {
            System.out.println(sql);
        }
    }
}
