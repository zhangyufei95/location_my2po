package com.example.cdc;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kudu.client.*;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import org.apache.log4j.Logger;

public class Location {
    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(Location.class);
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
//        tableEnv.executeSql(
//                "CREATE TABLE pgsql_source1 (\n" +
//                        " truck_id string,\n" +
//                        " `time` timestamp,\n" +
//                        " lng int,\n" +
//                        " lat int,\n" +
//                        " speed int,\n" +
//                        " course int,\n" +
//                        " distince int\n" +
//                        ") WITH (\n" +
//                        " 'connector' = 'postgres-cdc',\n" +
//                        " 'hostname' = '172.27.0.119',\n" +
//                        " 'port' = '5432',\n" +
//                        " 'username' = 'algo_pgm',\n" +
//                        " 'password' = 'klsy8#$1qsA7',\n" +
//                        " 'database-name' = 'postgres',\n" +
//                        " 'schema-name' = 'public',\n" +
//                        " 'debezium.snapshot.mode' = 'initial',\n" +
//                        " 'decoding.plugin.name' = 'pgoutput',\n" +
//                        " 'debezium.slot.name' = 'zyf0414',\n" +
//                        " 'table-name' = 'sinoiov_truck_location_v2_"+now_time1+"'\n" +
////                        " 'table-name' = 'sinoiov_truck_location_v2_20220414'\n" +
//                        ")"
//        );
//
//        tableEnv.executeSql(
//                "CREATE TABLE pgsql_source2 (\n" +
//                        " truck_id bigint,\n" +
//                        " mobile string,\n" +
//                        " lng string,\n" +
//                        " lat string,\n" +
//                        " gps_time timestamp\n" +
//                        ") WITH (\n" +
//                        " 'connector' = 'postgres-cdc',\n" +
//                        " 'hostname' = '172.27.0.119',\n" +
//                        " 'port' = '5432',\n" +
//                        " 'username' = 'algo_pgm',\n" +
//                        " 'password' = 'klsy8#$1qsA7',\n" +
//                        " 'database-name' = 'postgres',\n" +
//                        " 'schema-name' = 'public',\n" +
//                        " 'debezium.snapshot.mode' = 'initial',\n" +
//                        " 'decoding.plugin.name' = 'pgoutput',\n" +
//                        " 'debezium.slot.name' = 'test6',\n" +
//                        " 'table-name' = 'mp_app_location'\n" +
//                        ")"
//        );

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wx_tag (id bigint,status int,c_id bigint,name string) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '13336', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_user', " +
                "  'table-name' = 'wx_tag', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', " +
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");


        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_truck (id bigint,status int,owner string,plate string,main_driver_id bigint,contact_phone string) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '13331', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_truck', " +
                "  'table-name' = 'truck', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', " +
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wx_external_platform_mapping (id bigint,mobile string,real_name string,plate string,truck_id bigint,update_time string) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '13332', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_user', " +
                "  'table-name' = 'wx_external_platform_mapping', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', " +
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wx_tag_relation (tag_id bigint,create_time timestamp,user_tag_id bigint,status int,foreign_id bigint) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '13333', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_user', " +
                "  'table-name' = 'wx_tag_relation', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', " +
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");


        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_driver_users (mobile string,status int,create_time string,name string,id bigint) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '13335', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_user', " +
                "  'table-name' = 'driver_users', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', " +
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_travel_order (truck_id bigint,status int,create_time timestamp) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '13334', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_travel', " +
                "  'table-name' = 'travel_order', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', " +
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

//        Table tableQuery1 = tableEnv.sqlQuery("select pg_location.plate as plate,du.mobile as mobile,du.name as name,pg_location.lng as lng,pg_location.lat as lat,' ' as empty_city_id,' ' as release_date,' '  as intention_city_id,if(oto.truck_id is not null,'1','0') as is_cooperatin,pg_location.create_time as create_time from\n" +
//                "(select ifnull(pgsql_source11.truck_id,pgsql_source22.plate) as plate,ifnull(pgsql_source11.lng,pgsql_source22.lng) as lng,ifnull(pgsql_source11.lat,pgsql_source22.lat) as lat,ifnull(pgsql_source11.create_time,pgsql_source22.create_time) as create_time from (select *,date_format(`time`,'yyyy-MM-dd HH:mm:ss') as create_time from pgsql_source1 where date_format(`time`,'yyyy-MM-dd')=date_format(CURRENT_TIMESTAMP,'yyyy-MM-dd')) pgsql_source11 full outer join (select bb.plate,aa.truck_id,aa.mobile,aa.lng,aa.lat,aa.create_time from (select truck_id,mobile,round(cast(ifnull(lng,'0') as decimal(20,6))*600000) as lng,round(cast(ifnull(lat,'0') as decimal(20,6))*600000) as lat,date_format(gps_time,'yyyy-MM-dd HH:mm:ss') as create_time from pgsql_source2 where date_format(gps_time,'yyyy-MM-dd')=date_format(CURRENT_TIMESTAMP,'yyyy-MM-dd')) aa left join (select id,plate,main_driver_id,status from source_truck where status=1 and trim(plate)<>'') bb on aa.truck_id=bb.id) pgsql_source22 on pgsql_source11.truck_id=pgsql_source22.plate) pg_location\n" +
//                "left join\n" +
//                "(select id,plate,main_driver_id,status from source_truck where status=1 and trim(plate)<>'')truck\n" +
//                "on pg_location.plate=truck.plate\n" +
//                "left join\n" +
//                "(select truck_id,create_time,status from source_travel_order where date_format(create_time,'%Y-%m-%d') >=DATE_FORMAT(TIMESTAMPADD(DAY,-3,CURRENT_TIMESTAMP),'yyyy-MM-dd') and status=1) oto\n" +
//                "on oto.truck_id=truck.id\n" +
//                "left join\n" +
//                "(select mobile,SPLIT_INDEX(max(concat(create_time,'|',name,'|',cast(id as string))),'|',1) as name,cast(SPLIT_INDEX(max(concat(create_time,'|',name,'|',cast(id as string))),'|',2) as bigint) as id from source_driver_users where status=1 and trim(mobile)<>''  group by mobile) du\n" +
//                "on truck.main_driver_id=du.id");


        Table tableQuery2 = tableEnv.sqlQuery("select hot_tag.plate,hot_tag.mobile,hot_tag.name,0 as lng,0 as lat,hot_tag.empty_city_id,hot_tag.release_date,hot_tag.intention_city_id,'0' as is_cooperating,date_format(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss') as create_time from\n" +
                "(select * from\n" +
                "(\n" +
                "SELECT\n" +
                "   m.plate,\n" +
                "   m.mobile, \n" +
                "   max(m.real_name) as name,\n" +
                "   max(r.create_time) as create_time,\n" +
                "   max(if(r.user_tag_id=10000,from_unixtime(cast(cast(t.name as bigint)/1000 as int),'yyyy-MM-dd'),' ')) as  release_date,\n" +
                "   collect(distinct if(r.user_tag_id=10001,if(r.status=0,' ',cast(t.c_id as string)),' ')) as empty_city_id,\n" +
                "   collect(distinct if(r.user_tag_id=10002,if(r.status=0,' ',cast(t.c_id as string)),' ')) as intention_city_id\n" +
                "FROM\n" +
                "    (select tag_id,create_time,user_tag_id,status,foreign_id from source_wx_tag_relation where create_time >=DATE_FORMAT(TIMESTAMPADD(DAY,-14,CURRENT_TIMESTAMP),'yyyy-MM-dd'))  r \n" +
                "inner join (select id,status,c_id,name from source_wx_tag) t on r.tag_id=t.id and t.`status`=1\n" +
                "inner join (select mobile,plate,cast(SPLIT_INDEX(value1,'|',1) as bigint) as id,SPLIT_INDEX(value1,'|',2) as real_name from (select mobile,plate,max(concat(update_time,'|',cast(id as string),'|',real_name)) as value1 from source_wx_external_platform_mapping where truck_id>0 and trim(mobile)<>'' group by mobile,plate)tmp ) m on m.id=r.foreign_id\n" +
                //  "inner join (select id,plate,owner,main_driver_id,contact_phone from source_truck where status=1 and trim(plate)<>'')truck1\n" +
                //  "on m.truck_id = truck1.id and m.real_name=truck1.owner and m.mobile=truck1.contact_phone\n" +
                "WHERE\n" +
                "    r.user_tag_id in (10001,10000,10002) and trim(m.plate)<>'' and trim(m.mobile)<>'' \n" +
                "group by m.plate,m.mobile) hot_tag1 where release_date >=DATE_FORMAT(TIMESTAMPADD(DAY,0,CURRENT_TIMESTAMP),'yyyy-MM-dd')) hot_tag");
        //"where hot_tag.release_date >='2022-04-12'");

//        Table tableQuery2 = tableEnv.sqlQuery("select hot_tag.plate,hot_tag.mobile,hot_tag.name,0 as lng,0 as lat,hot_tag.empty_city_id,hot_tag.release_date,hot_tag.intention_city_id,'0' as is_cooperating,date_format(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss') as create_time from\n" +
//                "(select * from\n" +
//                "(\n" +
//                "SELECT\n" +
//                "   m.plate,\n" +
//                "   m.mobile, \n" +
//                "   max(m.real_name) as name,\n" +
//                "   max(r.create_time) as create_time,\n" +
//                "   max(if(r.user_tag_id=10000,from_unixtime(cast(cast(t.name as bigint)/1000 as int),'yyyy-MM-dd'),' ')) as  release_date,\n" +
//                "   collect(distinct if(r.user_tag_id=10001,if(r.status=0,' ',cast(t.c_id as string)),' ')) as empty_city_id,\n" +
//                "   collect(distinct if(r.user_tag_id=10002,if(r.status=0,' ',cast(t.c_id as string)),' ')) as intention_city_id\n" +
//                "FROM\n" +
//                "    (select tag_id,create_time,user_tag_id,status,foreign_id from source_wx_tag_relation where create_time >=DATE_FORMAT(TIMESTAMPADD(DAY,-14,CURRENT_TIMESTAMP),'yyyy-MM-dd'))  r \n" +
//                "inner join (select id,status,c_id,name from source_wx_tag) t on r.tag_id=t.id and t.`status`=1\n" +
//                "inner join (select mobile,plate,cast(SPLIT_INDEX(value1,'|',1) as bigint) as id,SPLIT_INDEX(value1,'|',2) as real_name from (select mobile,plate,max(concat(update_time,'|',cast(id as string),'|',real_name)) as value1 from source_wx_external_platform_mapping where truck_id>0 and trim(mobile)<>'' group by mobile,plate)tmp ) m on m.id=r.foreign_id\n" +
//                //  "inner join (select id,plate,owner,main_driver_id,contact_phone from source_truck where status=1 and trim(plate)<>'')truck1\n" +
//                //  "on m.truck_id = truck1.id and m.real_name=truck1.owner and m.mobile=truck1.contact_phone\n" +
//                "WHERE\n" +
//                "    r.user_tag_id in (10001,10000,10002) and trim(m.plate)<>'' and trim(m.mobile)<>'' \n" +
//                "group by m.plate,m.mobile) hot_tag1 where release_date >=DATE_FORMAT(TIMESTAMPADD(DAY,0,CURRENT_TIMESTAMP),'yyyy-MM-dd')) hot_tag");
//        //"where hot_tag.release_date >='2022-04-12'");

//        tableEnv.toRetractStream(tableQuery1, Row.class).filter(
//                new FilterFunction<Tuple2<Boolean, Row>>() {
//                    @Override
//                    public boolean filter(Tuple2<Boolean, Row> booleanRowTuple2) throws Exception {
//                        return booleanRowTuple2.f0;
//                    }
//                }
//        ).map(new MapFunction<Tuple2<Boolean, Row>, Row>() {
//            @Override
//            public Row map(Tuple2<Boolean, Row> booleanRowTuple2) throws Exception {
//                return booleanRowTuple2.f1;
//            }
//        }).addSink(new Location.KuduSink());

        tableEnv.toRetractStream(tableQuery2, Row.class).filter(
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
        }).addSink(new Location.KuduSink());

        exeEnv.execute("中交、小程序和腾讯实时轨迹与司机空车信息融合表");
    }
    public static class KuduSink extends RichSinkFunction<Row> {
        private KuduTable table = null;
        private KuduClient client = null;
        private KuduSession session = null;
        private  String masteraddr = "172.27.16.41:7051,172.27.16.43:7051,172.27.16.49:7051";
        public void open(Configuration parameters) throws Exception {
            //创建kudu连接
            client = new KuduClient.KuduClientBuilder(masteraddr).defaultSocketReadTimeoutMs(6000).build();
            //打开表
            table = client.openTable("stage.stage_driver_location");
            // 创建写session,kudu必须通过session写入
            session = client.newSession();
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
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String now_time = sdf.format(new java.util.Date());
            String mobile="";
            String plate="";
            String owner="";
            int latest_lng=0;
            int latest_lat=0;
            String release_city="";
            String release_time="";
            String prefer_city="";
            String create_time=now_time;
            int is_cooperated=0;
            try {
                if(null==value.getField(1)){
                    mobile="";
                } else {
                    mobile = value.getField(1).toString();
                }
                if(null==value.getField(0)){
                    plate="";
                } else {
                    plate = value.getField(0).toString();
                }
                if (null==value.getField(2)){
                    owner="";
                } else {
                    owner = value.getField(2).toString();
                }
                if(null==value.getField(3) || value.getField(3).equals("")){
                    latest_lng=0;
                } else if(value.getField(3) !=null && !(value.getField(3).equals(""))){
                    latest_lng = Integer.parseInt(value.getField(3).toString());
                } else {
                    latest_lng=0;
                }
                if(null==value.getField(4) || value.getField(4).equals("")){
                    latest_lat=0;
                } else if(value.getField(4) !=null && !(value.getField(4).equals(""))){
                    latest_lat = Integer.parseInt(value.getField(4).toString());
                } else {
                    latest_lat=0;
                }
                if (null==value.getField(5)){
                    release_city="";
                } else {
                    release_city = value.getField(5).toString();
                }

                if (null==value.getField(6)){
                    release_time="";
                } else {
                    release_time = value.getField(6).toString();
                }

                if (null==value.getField(7)){
                    prefer_city="";
                } else {
                    prefer_city = value.getField(7).toString();
                }

                if(null==value.getField(8) || value.getField(8).equals("")){
                    is_cooperated=0;
                } else if(value.getField(8) !=null && !(value.getField(8).equals(""))) {
                    is_cooperated =Integer.parseInt(value.getField(8).toString());
                } else {
                    is_cooperated=0;
                }

                if (null==value.getField(9)){
                    create_time=now_time;
                } else {
                    create_time = value.getField(9).toString();
                }

                String result_lng = "";
                String result_lat = "";
                float num_lng =(float)latest_lng/600000;
                float num_lat =(float)latest_lat/600000;
                DecimalFormat df = new DecimalFormat("0.00000");
                result_lng = df.format(num_lng);
                result_lat = df.format(num_lat);
//                System.out.println("处理过后的lng: "+result_lng+"; lat: "+result_lat);
                String aa = "";
//                String regEx = "[{ =1}]";
                String regEx = "(=1| |\\{|\\})";
                session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
                session.setTimeoutMillis(60000);
                String release_city1 = release_city.replaceAll(regEx, aa);
                String prefer_city1 = prefer_city.replaceAll(regEx, aa);
                Upsert upsert = table.newUpsert();
                PartialRow row2 = upsert.getRow();
                row2.addString("mobile",mobile);
                row2.addString("plate",plate);
                row2.addString("owner",owner);
                row2.addString("latest_lng",result_lng);
                row2.addString("latest_lat",result_lat);
                row2.addString("create_time",create_time);
                if (release_city1.indexOf(",") == 0) {
                    String release_city2 = release_city1.substring(1);
                    row2.addString("release_city", release_city2);
                } else if ((release_city1.lastIndexOf(',') != -1) && (release_city1.lastIndexOf(',') + 1 == release_city1.length())) {
                    String release_city2 = release_city1.substring(0, release_city1.length() - 1);
                    row2.addString("release_city", release_city2);
                } else {
                    row2.addString("release_city", " ");
                }
                //   row2.addString("release_city",release_city);
                if(release_time.length()>0) {
                    row2.addString("release_time", release_time);
                } else {
                    row2.addString("release_time", " ");
                }

                if (prefer_city1.indexOf(",") == 0) {
                    String prefer_city2 = prefer_city1.substring(1);
                    row2.addString("prefer_city", prefer_city2);
                } else if ((prefer_city1.lastIndexOf(',') != -1) && (prefer_city1.lastIndexOf(',') + 1 == prefer_city1.length())) {
                    String prefer_city2 = prefer_city1.substring(0, prefer_city1.length() - 1);
                    row2.addString("prefer_city", prefer_city2);
                } else {
                    row2.addString("prefer_city", " ");
                }
                // row2.addString("prefer_city",prefer_city);
                row2.addInt("is_cooperated",is_cooperated);
                logger.info(row2.toString());
                session.apply(upsert);
                session.flush();
            } catch (Exception e) {
                logger.error(e.getMessage(),e);
                e.printStackTrace();
            }
//            System.out.println("【"+now_time+"】数据流为： "+value.toString());
        }
    }
}
