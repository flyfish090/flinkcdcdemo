package org.chinasoftinc.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQL_CDC {

    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        System.out.println(tableEnv);

        //2.创建 Flink-MySQL-CDC 的 Source
        tableEnv.executeSql("CREATE TABLE tb_products_cdc (" + " id INT," +
                " name STRING," +
                " description STRING" +
                ") WITH (" +
                " 'connector' = 'mysql-cdc'," +
                " 'hostname' = '47.122.7.128'," +
                " 'port' = '3306'," +
                " 'username' = 'root'," +
                " 'password' = 'qida@123'," +
                " 'database-name' = 'db_inventory_cdc'," +
                " 'table-name' = 'tb_products_cdc'" +
                ")")
        ;
        tableEnv.executeSql("select * from tb_products_cdc").print();
        env.execute();

    }

}
