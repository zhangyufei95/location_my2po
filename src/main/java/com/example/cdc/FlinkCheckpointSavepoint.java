package com.example.cdc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
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

public class FlinkCheckpointSavepoint {

    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(FlinkCheckpointSavepoint.class);

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
        exeEnv.setStateBackend(new FsStateBackend("hdfs://HDFS17492/flink/checkpoints"));
        exeEnv.setParallelism(1);
        //表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(exeEnv, fsSettings);



        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  ods_demand_car_require (id bigint,demand_id bigint,car_length decimal(10,2),car_type int,type int,status int,update_time timestamp,create_time timestamp(3),WATERMARK FOR create_time as create_time - INTERVAL '10' SECOND) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '31669', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_user', " +
                "  'table-name' = 'demand_car_require', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', " +
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

        Table tableQuery3 = tableEnv.sqlQuery("select demand_id,create_time from ods_demand_car_require where status =1 and left(cast(create_time as string),10)>'2022-07-20'");

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
        data2.addSink(new KuduSink11());
        exeEnv.execute("外呼司机基本信息&热标签&聊价_0719_需求信息");
    }

    public static class KuduSink11 extends RichSinkFunction<Row> {
        private KuduTable table = null;
        private KuduTable table1 = null;
        private KuduClient client = null;
        private KuduSession session = null;
        private String masteraddr = "172.27.16.41:7051,172.27.16.43:7051,172.27.16.49:7051";

        public void open(Configuration parameters) throws Exception {
            //创建kudu连接
            client = new KuduClient.KuduClientBuilder(masteraddr).defaultSocketReadTimeoutMs(6000).build();
            //打开表
            table = client.openTable("stage.stage_demand_car");
            // 创建写session,kudu必须通过session写入
            session = client.newSession();
            session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
            session.setTimeoutMillis(60000);
        }

        @Override
        public void close() throws Exception {
            if (session != null) {
                session.close();
            }
            if (client != null) {
                client.close();
            }
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
            String create_time = value.getField(1).toString();
            String demand_id = value.getField(0).toString();
            System.out.println(value.toString());
            Upsert upsert = table.newUpsert();
            PartialRow row22 = upsert.getRow();
            row22.addString("demand_id", demand_id);
            row22.addString("create_time", create_time);
            session.apply(upsert);
            session.flush();
        }
    }
}
