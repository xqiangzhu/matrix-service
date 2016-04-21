package com.cubead.matrix.providertest.base;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

public class PartitionQueryTest extends BaseTest {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public static String createPartitionSql;
    private int batchInsertOnce = 1000;
    private final static int pacell_thread = 10000;
    private CountDownLatch countDownLatch = new CountDownLatch(pacell_thread);
    private final static String random_date_start = "2015-4-21";
    private final static String random_date_end = "2016-4-21";
    private final static String[] names = new String(
            "There    are moments in life when you miss someone so much that you just want to pick them from your dreams and hug them for real "
                    + "Dream what you want to dream go where you want to go be what you want to be because you have only one life and one chance "
                    + "to do all the things you want to do "
                    + "May you have enough happiness to make you sweet,enough trials to make you strongenough sorrow to keep you human "
                    + "enough hope to make you happy Always put yourself in others shoes If you feel that it hurts you it probably hurts the other person "
                    + "too The happiest of people don t necessarily have the best of everything they just make the most of everything that comes along their way "
                    + "Happiness lies for those who cry those who hurt those who have searched "
                    + "and those who have tried for only they can appreciate the importance of people who have touched their lives "
                    + "Love begins with a smile grows with a kiss and ends with a tear The brightest future will always be based "
                    + "on a forgotten past,you cant go on well in lifeuntil you let go of your past failures and heartaches ")
            .replaceAll("[,|.|’|;|?]", " ").replaceAll("\\s{1,}", " ").split(" ");

    private AtomicInteger idAtomicInteger = new AtomicInteger(1);
    static ExecutorService executorService = Executors.newFixedThreadPool(10);

    @Before
    public void initz() {
        createPartitionSql = "CREATE TABLE ca_sumary_partion_sampale "
                + "(id INT NOT NULL,dimension_a VARCHAR(30), dimension_b VARCHAR(30), log_day DATE NOT NULL, "
                + "log_day_other DATE NOT NULL , quato_a INT  NOT NULL,   quato_b INT  NOT NULL)"
                + "  PARTITION BY RANGE ( MONTH(log_day))(  "
                + "  PARTITION p1_ VALUES LESS THAN (2), PARTITION p2_ VALUES LESS THAN (3),"
                + "  PARTITION p3_ VALUES LESS THAN (4), PARTITION p4_ VALUES LESS THAN (5),"
                + "  PARTITION p5_ VALUES LESS THAN (6), PARTITION p6_ VALUES LESS THAN (7),"
                + "  PARTITION p7_ VALUES LESS THAN (8), PARTITION p8_ VALUES LESS THAN (9),"
                + "  PARTITION p9_ VALUES LESS THAN (10), PARTITION p10_ VALUES LESS THAN (11),"
                + "  PARTITION p11_ VALUES LESS THAN (12), PARTITION p12_ VALUES LESS THAN (13))";
    }

    // @Test
    public void createPartionTable() {
        jdbcTemplate.execute(createPartitionSql);
    }

    public static void main(String[] args) {

        createPartitionSql = " ALTER TABLE ca_summary_136191_compressed_2yr  PARTITION BY RANGE ( log_day)(  "
                + "  PARTITION p1_ VALUES LESS THAN (89), PARTITION p2_ VALUES LESS THAN (119),"
                + "  PARTITION p3_ VALUES LESS THAN (149), PARTITION p4_ VALUES LESS THAN (179),"
                + "  PARTITION p5_ VALUES LESS THAN (209), PARTITION p6_ VALUES LESS THAN (239),"
                + "  PARTITION p7_ VALUES LESS THAN (269), PARTITION p8_ VALUES LESS THAN (299),"
                + "  PARTITION p9_ VALUES LESS THAN (329), PARTITION p10_ VALUES LESS THAN (359),"
                + "  PARTITION p11_ VALUES LESS THAN (389), PARTITION p12_ VALUES LESS THAN (419))";

        System.out.println(createPartitionSql);
        for (int i = 59; i < 418; i += 30) {
            System.out.println(i + 30);
        }

    }

    @Test
    public void initzPartitionDatas() {

        for (int i = 0; i < pacell_thread; i++) {
            executorService.execute(new Runnable() {
                public void run() {
                    insertContractAch(generterRandEmployees());
                    countDownLatch.countDown();
                }
            });
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        executorService.shutdown();
    }

    public void insertContractAch(final List<Employee> list) {

        String sql = "INSERT INTO ca_sumary_partion_sampale(id, dimension_a, dimension_b, log_day, log_day_other, quato_a, quato_b) "
                + "VALUES(?, ?, ?, ?, ?, ?, ?) ";
        try {
            this.getJdbcTemplate().batchUpdate(sql, new MyBatchPreparedStatementSetter(list));
        } catch (DataAccessException e) {
            e.printStackTrace();
        }

    }

    private class MyBatchPreparedStatementSetter implements BatchPreparedStatementSetter {

        final List<Employee> temList;

        public MyBatchPreparedStatementSetter(List<Employee> list) {
            temList = list;
        }

        public int getBatchSize() {
            return temList.size();
        }

        public void setValues(PreparedStatement ps, int i) throws SQLException {
            Employee contractAchVO = (Employee) temList.get(i);
            ps.setInt(1, idAtomicInteger.getAndIncrement());
            ps.setString(2, contractAchVO.getFname());
            ps.setString(3, contractAchVO.getLname());
            ps.setString(4, contractAchVO.getHired());
            ps.setString(5, contractAchVO.getSeparated());
            ps.setInt(6, contractAchVO.getJob_code());
            ps.setInt(7, contractAchVO.getStore_id());
        }
    }

    public List<Employee> generterRandEmployees() {

        List<Employee> employees = new ArrayList<>(batchInsertOnce);
        for (int i = 0; i < batchInsertOnce; i++) {
            Employee employee = new Employee();
            employee.setHired(randomDateAsStr(random_date_start, random_date_end));
            employee.setSeparated(randomDateAsStr(random_date_start, random_date_end));
            employee.setStore_id(new Random().nextInt(20) + 1);
            employee.setJob_code(new Random().nextInt(100000));
            employee.setFname(names[new Random().nextInt(names.length)].trim());
            employee.setLname(names[new Random().nextInt(names.length)].trim());
            employees.add(employee);
        }
        return employees;
    }

    private static String randomDateAsStr(String beginDate, String endDate) {
        Date d = randomDate(beginDate, endDate);
        String dateStr = DateFormat.getDateInstance(DateFormat.DEFAULT).format(d);
        return dateStr;
    }

    private static Date randomDate(String beginDate, String endDate) {

        try {

            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            Date start = format.parse(beginDate);// 构造开始日期
            Date end = format.parse(endDate);// 构造结束日期

            if (start.getTime() >= end.getTime()) {
                return null;
            }

            long date = random(start.getTime(), end.getTime());
            return new Date(date);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;

    }

    private static long random(long begin, long end) {

        long rtn = begin + (long) (Math.random() * (end - begin));
        // 如果返回的是开始时间和结束时间，则递归调用本函数查找随机值
        if (rtn == begin || rtn == end) {
            return random(begin, end);
        }
        return rtn;

    }

    public class Employee {
        private int id;
        private String fname;
        private String lname;
        private String hired;
        private String separated;
        private int job_code;
        private int store_id;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getFname() {
            return fname;
        }

        public void setFname(String fname) {
            this.fname = fname;
        }

        public String getLname() {
            return lname;
        }

        public void setLname(String lname) {
            this.lname = lname;
        }

        public String getHired() {
            return hired;
        }

        public void setHired(String hired) {
            this.hired = hired;
        }

        public String getSeparated() {
            return separated;
        }

        public void setSeparated(String separated) {
            this.separated = separated;
        }

        public int getJob_code() {
            return job_code;
        }

        public void setJob_code(int job_code) {
            this.job_code = job_code;
        }

        public int getStore_id() {
            return store_id;
        }

        public void setStore_id(int store_id) {
            this.store_id = store_id;
        }

    }

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

}
