package com.example.cdc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.cdc.utils.ReadFile;
import com.example.cdc.utils.Sink2Kudu;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kudu.client.*;

import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.*;

public class HtCaller0 {

    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(HtCaller0.class);
    public static void main(String[] args) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String now_time1 = sdf.format(new java.util.Date());
        //设置flink表环境变量
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        //获取flink流环境变量
        StreamExecutionEnvironment exeEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        exeEnv.setParallelism(1);
        //表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(exeEnv, fsSettings);
//        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  ods_users ( id bigint,user_name string,user_role_id bigint,password string,users_department_id bigint,user_group_id bigint,status int,city_id int,avatar string,last_login_time string,all_trucks int,name string,mobile string,on_duty string,create_time timestamp,update_time timestamp,create_users_id bigint,update_users_id bigint,wechat_user_name string,last_login_ip string,rules string,company_id int,region string,region_id bigint,region_group_Id bigint,saler_group int,job_number string,system_type int,stop_time string,proc_time AS PROCTIME() ) with( 'connector' = 'jdbc', 'driver'='com.mysql.cj.jdbc.Driver',  'username' = 'flink_cdc_ad',   'password' = 'jklsek52@=9H',   'table-name' = 'users',  'url' = 'jdbc:mysql://172.27.0.48:3306/ht_user', 'lookup.cache.max-rows' = '1000', 'lookup.cache.ttl' = '10000')");
//        Table tableQuery3 = tableEnv.sqlQuery("select * from ods_users");
        tableEnv.executeSql(ReadFile.SqlContent("source_logger_yewu.sql",1,"algo","source"));
        System.out.println(ReadFile.SqlContent("source_logger_yewu.sql",1,"algo","source"));
//        tableEnv.executeSql(ReadFile.SqlContent("source_travel_order.sql",1,"ht","source"));
//        System.out.println(ReadFile.SqlContent("source_travel_order_external_cost.sql",1,"ht","source"));
        System.out.println(ReadFile.SqlContent("trans.sql",1,"algo","query"));
        Table tableQuery3 = tableEnv.sqlQuery(ReadFile.SqlContent("trans.sql",1,"algo","query"));


//        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  ods_contracted_route (id bigint,type int,unit int,status int,demand_id bigint,order_shipment int,customer_id bigint,use_car_start timestamp,goods_weight string,create_time timestamp) with(" +
//                " 'connector' = 'mysql-cdc', " +
//                " 'hostname' = '172.27.0.48', " +
//                " 'server-id' = '44561', " +
//                "  'port' = '3306', " +
//                "  'username' = 'flink_cdc_ad', " +
//                "  'password' = 'jklsek52@=9H', " +
//                "  'database-name' = 'ht_user', " +
//                "  'table-name' = 'contracted_route', " +
//                "  'server-time-zone' = 'Asia/Shanghai', " +
//                "  'debezium.snapshot.locking.mode' = 'none', " +
//                "  'scan.incremental.snapshot.enabled' = 'false', " +
//                "  'debezium.snapshot.mode' = 'initial' " +
//                ")");
//
//        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  ods_base_geo_info (id bigint,parent_id bigint,name string,type int,proc_time AS PROCTIME() ) with(" +
//                " 'connector' = 'jdbc', " +
//                "'driver'='com.mysql.cj.jdbc.Driver',"+
//                "  'username' = 'flink_cdc_ad', " +
//                "  'password' = 'jklsek52@=9H', " +
//                "  'table-name' = 'base_geo_info', " +
//                " 'url' = 'jdbc:mysql://172.27.0.48:3306/ht_travel'," +
//                " 'lookup.cache.max-rows' = '1000'," +
//                " 'lookup.cache.ttl' = '10000'"+
//                ")");
//
//
//        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  ods_contracted_route_info (route_id bigint,type int,sequence_number int,city_id bigint,status int,create_time timestamp) with(" +
//                " 'connector' = 'mysql-cdc', " +
//                " 'hostname' = '172.27.0.48', " +
//                " 'server-id' = '44562', " +
//                "  'port' = '3306', " +
//                "  'username' = 'flink_cdc_ad', " +
//                "  'password' = 'jklsek52@=9H', " +
//                "  'database-name' = 'ht_user', " +
//                "  'table-name' = 'contracted_route_info', " +
//                "  'server-time-zone' = 'Asia/Shanghai', " +
//                "  'debezium.snapshot.locking.mode' = 'none', " +
//                "  'scan.incremental.snapshot.enabled' = 'false', " +
//                "  'debezium.snapshot.mode' = 'initial' " +
//                ")");

//        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  ods_logger_yewu (id int,sessionId string,version string,input_msg string) with(" +
//                " 'connector' = 'mysql-cdc', " +
//                " 'hostname' = '172.27.16.38', " +
//                " 'server-id' = '74564', " +
//                "  'port' = '3306', " +
//                "  'username' = 'root', " +
//                "  'password' = 'Zhicang@2017', " +
//                "  'database-name' = 'algo_nlp', " +
//                "  'table-name' = 'logger_yewu', " +
//                "  'server-time-zone' = 'Asia/Shanghai', " +
//                "  'debezium.snapshot.locking.mode' = 'none', " +
//                "  'scan.incremental.snapshot.enabled' = 'false', " +
//                "  'debezium.snapshot.mode' = 'initial' " +
//                ")");


//        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  ods_price_log (sessionId string,ask int,bid int,deal tinyint,finish tinyint,price int,update_time string,create_time string,final_intent string,turn int,ask_turn int,msg_fee int,first_ask int,first_bid int,call_type int,gd_distance decimal(10,2), estimate_cost int) with(" +
//                " 'connector' = 'mysql-cdc', " +
//                " 'hostname' = '172.27.16.38', " +
//                " 'server-id' = '29564', " +
//                "  'port' = '3306', " +
//                "  'username' = 'root', " +
//                "  'password' = 'Zhicang@2017', " +
//                "  'database-name' = 'algo_nlp', " +
//                "  'table-name' = 'price_log', " +
//                "  'server-time-zone' = 'Asia/Shanghai', " +
//                "  'debezium.snapshot.locking.mode' = 'none', " +
//                "  'scan.incremental.snapshot.enabled' = 'false', " +
//                "  'debezium.snapshot.mode' = 'initial' " +
//                ")");

//        tableEnv.executeSql(
//                "CREATE TABLE ods_call_sobot_task_detail (\n" +
//                        " id int, \n" +
//                        " demand_id bigint, \n" +
//                        " called_number string, \n" +
//                        " called_name string, \n" +
//                        " mobile string, \n" +
//                        " status int, \n" +
//                        " out_id string, \n" +
//                        " answer_time bigint, \n" +
//                        " end_time int, \n" +
//                        " intention_list string, \n" +
//                        " call_content string, \n" +
//                        " voice_url string, \n" +
//                        " type int, \n" +
//                        " intention_results string, \n" +
//                        " create_time timestamp, \n" +
//                        " update_time timestamp, \n" +
//                        " sobot_task_id string, \n" +
//                        " task_id int \n" +
//                        ") WITH (\n" +
//                        " 'connector' = 'postgres-cdc',\n" +
//                        " 'hostname' = '172.27.0.119',\n" +
//                        " 'port' = '5432',\n" +
//                        " 'username' = 'algo_pgm',\n" +
//                        " 'password' = 'klsy8#$1qsA7',\n" +
//                        " 'database-name' = 'ht_caller',\n" +
//                        " 'schema-name' = 'public',\n" +
//                        " 'debezium.snapshot.mode' = 'initial',\n" +
//                        " 'decoding.plugin.name' = 'pgoutput',\n" +
//                        " 'debezium.slot.name' = 'ht_caller0803',\n" +
//                        " 'table-name' = 'call_sobot_task_detail'\n" +
//                        ")"
//        );
//        Table tableQuery3 = tableEnv.sqlQuery("select sessionId,price,estimate_cost,gd_distance from ods_price_log");

//        Table tableQuery3 = tableEnv.sqlQuery("select demand_id,unit,goods_weight,use_car_start,load_city_name,load_city_name as first_load_city_name,unload_city_name as last_unload_city_name,concat(load_city_name,'-',unload_city_name) as line_prefer from\n" +
//                "(select cr.demand_id,cr.unit,cr.goods_weight,cr.use_car_start,cri.load_city_id,geo.name as load_city_name,cri.unload_city_id,geo1.name as unload_city_name from\n" +
//                "(select id,demand_id,unit,use_car_start,goods_weight from ods_contracted_route where left(cast(create_time as string),10) >= '2022-07-01' and type=3 and status =1 and demand_id is not null) cr \n" +
//                "left join \n" +
//                "(select route_id,cast(split_index(min(concat(cast(type as string),'|',cast(sequence_number as string),'|',cast(city_id as string))),'|',2) as bigint) as load_city_id,cast(split_index(max(concat(cast(type as string),'|',cast(sequence_number as string),'|',cast(city_id as string))),'|',2) as bigint) as unload_city_id from ods_contracted_route_info where left(cast(create_time as string),10)>='2022-07-01' and status =1 group by route_id) cri\n" +
//                "on cr.id = cri.route_id\n" +
//                "left join\n" +
//                "(select * from ods_base_geo_info) geo \n" +
//                "on cri.load_city_id = geo.id\n" +
//                "left join\n" +
//                "(select * from ods_base_geo_info) geo1\n" +
//                "on cri.unload_city_id = geo1.id\n" +
//                ") base_table1");



        DataStream<Row> data2 = tableEnv.toRetractStream(tableQuery3, Row.class).filter(
                new FilterFunction<Tuple2<Boolean, Row>>() {
                    @Override
                    public boolean filter(Tuple2<Boolean, Row> booleanRowTuple2) throws Exception {
                        return booleanRowTuple2.f0;
                    }
                }
        ).map(new MapFunction<Tuple2<Boolean, Row>, Row>() {
            @Override
            public Row map(Tuple2<Boolean, Row> booleanRowTuple2) throws Exception {
                return booleanRowTuple2.f1;
            }
        });
//                returns(Types.ROW(Types.STRING,Types.DOUBLE,Types.DOUBLE,Types.DOUBLE)).filter(s -> {
//            if (null!=s.getField(1))
//            { return true; } else { return false; }
//        });
        data2.print();
//                .addSink(new com.example.cdc.utils.Sink2Kudu());
//                addSink(new KuduSink11());
        exeEnv.execute("外呼司机基本信息&热标签&聊价_0803");
    }

    public static class KuduSink11 extends RichSinkFunction<Row> {

        public void open(Configuration parameters) throws Exception {
        }

        @Override
        public void close() throws Exception {
        }


        /**
         * 数据输出时执行，每一个数据输出时，都会执行此方法
         *
         * @param value
         * @param context
         * @throws Exception
         */
        @Override
        public void invoke(Row value, Context context) throws Exception {
            System.out.println(value);
//            String sessionId = value.getField(0).toString();
//            String call_content = value.getField(1).toString();
//            call_content = call_content.replaceAll("([=]\\d*)", "");
//            System.out.println(call_content);
        }
    }
}
