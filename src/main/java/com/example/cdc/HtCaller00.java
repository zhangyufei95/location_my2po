package com.example.cdc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
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

public class HtCaller00 {

    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(HtCaller.class);
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
        exeEnv.setParallelism(3);
        //表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(exeEnv, fsSettings);
        tableEnv.executeSql(
                "CREATE TABLE ods_call_in_sobot_detail (\n" +
                        " id string, \n" +
                        " demand_id bigint, \n" +
                        " call_id string, \n" +
                        " call_type int, \n" +
                        " tel string, \n" +
                        " caller string, \n" +
                        " content string, \n" +
                        " answer_time timestamp, \n" +
                        " end_time timestamp, \n" +
                        " fee_duration int, \n" +
                        " intention_name string, \n" +
                        " caller_labels string, \n" +
                        " call_labels string, \n" +
                        " status int, \n" +
                        " create_time timestamp, \n" +
                        " update_time timestamp, \n" +
                        " voice_url string, \n" +
                        " answer_time timestamp\n" +
                        ") WITH (\n" +
                        " 'connector' = 'postgres-cdc',\n" +
                        " 'hostname' = '172.27.0.119',\n" +
                        " 'port' = '5432',\n" +
                        " 'username' = 'algo_pgm',\n" +
                        " 'password' = 'klsy8#$1qsA7',\n" +
                        " 'database-name' = 'ht_caller',\n" +
                        " 'schema-name' = 'public',\n" +
                        " 'debezium.snapshot.mode' = 'initial',\n" +
                        " 'decoding.plugin.name' = 'pgoutput',\n" +
                        " 'debezium.slot.name' = 'ht_caller0713',\n" +
                        " 'table-name' = 'call_in_sobot_detail'\n" +
                        ")"
        );

        tableEnv.executeSql(
                "CREATE TABLE ods_call_sobot_task_detail (\n" +
                        " id int, \n" +
                        " demand_id bigint, \n" +
                        " called_number string, \n" +
                        " called_name string, \n" +
                        " mobile string, \n" +
                        " status int, \n" +
                        " out_id string, \n" +
                        " answer_time bigint, \n" +
                        " end_time int, \n" +
                        " intention_list string, \n" +
                        " call_content string, \n" +
                        " voice_url string, \n" +
                        " type int, \n" +
                        " intention_results string, \n" +
                        " create_time timestamp, \n" +
                        " update_time timestamp, \n" +
                        " sobot_task_id string, \n" +
                        " task_id int \n" +
                        ") WITH (\n" +
                        " 'connector' = 'postgres-cdc',\n" +
                        " 'hostname' = '172.27.0.119',\n" +
                        " 'port' = '5432',\n" +
                        " 'username' = 'algo_pgm',\n" +
                        " 'password' = 'klsy8#$1qsA7',\n" +
                        " 'database-name' = 'ht_caller',\n" +
                        " 'schema-name' = 'public',\n" +
                        " 'debezium.snapshot.mode' = 'initial',\n" +
                        " 'decoding.plugin.name' = 'pgoutput',\n" +
                        " 'debezium.slot.name' = 'ht_caller0714',\n" +
                        " 'table-name' = 'call_sobot_task_detail'\n" +
                        ")"
        );
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  ods_driver_info (id int,phone string,IsTeam string,IsStop string,StartCity string,EndCity string,EmptyDate string,CarType int,CarLength double,RasaMessage string,sentence string,intent string,SessionId string,CreateTime string,UpdateTime string,IsTempStop int,SpaceTimeResult int) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.16.38', " +
                " 'server-id' = '21559', " +
                "  'port' = '3306', " +
                "  'username' = 'root', " +
                "  'password' = 'Zhicang@2017', " +
                "  'database-name' = 'algo_nlp', " +
                "  'table-name' = 'driver_info', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', " +
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  ods_non_standard_trans (session_id string,questions_list string,series_no bigint,statusType string,seriesNo string,contentType string,intent string,notes string,question string,create_time string) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.16.38', " +
                " 'server-id' = '21560', " +
                "  'port' = '3306', " +
                "  'username' = 'root', " +
                "  'password' = 'Zhicang@2017', " +
                "  'database-name' = 'algo_nlp', " +
                "  'table-name' = 'non_standard_trans', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', " +
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  ods_driver_info_input (id int,SafePhone string,StartCity string,EndCity string,EmptyDate string,CarType string,CarLength string,intent string,SessionId string,CreateTime string,UpdateTime string,RealPhone string,DemandId string,Goods string) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.16.38', " +
                " 'server-id' = '21563', " +
                "  'port' = '3306', " +
                "  'username' = 'root', " +
                "  'password' = 'Zhicang@2017', " +
                "  'database-name' = 'algo_nlp', " +
                "  'table-name' = 'driver_info_input', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', " +
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  ods_price_log (sessionId string,ask int,bid int,deal tinyint,finish tinyint,price int,update_time string,create_time string,final_intent string,turn int,ask_turn int,msg_fee int,first_ask int,first_bid int,call_type int) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.16.38', " +
                " 'server-id' = '21564', " +
                "  'port' = '3306', " +
                "  'username' = 'root', " +
                "  'password' = 'Zhicang@2017', " +
                "  'database-name' = 'algo_nlp', " +
                "  'table-name' = 'price_log', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', " +
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");




        Table tableQuery2 = tableEnv.sqlQuery("select call_table.sessionid,call_table.demand_id,call_table.mobile,\n" +
                "call_table.voice_url,call_table.intention_results,call_table.answer_time,call_table.call_type,\n" +
                "dr.IsTeam,dr.IsStop,if(call_table.call_type = 'out',dr.StartCity,'') as startcity,\n" +
                "if(call_table.call_type = 'out',dr.EndCity,'') as endcity,if(call_table.call_type = 'out',dr.EmptyDate,'') as emptydate,if(call_table.call_type = 'out',cast(dr.CarType as string),dri.CarType) as cartype,if(call_table.call_type = 'out',cast(dr.CarLength as string),dri.CarLength) as carlength,\n" +
                "if(call_table.call_type = 'out','',dri.Goods) as goods,\n" +
                "cast(pl.ask as string) as price,non_st.non_st,dr.IsTempStop,DATE_FORMAT(call_table.create_time,'yyyy-MM-dd HH:mm:ss') as create_time\n" +
                "from\n" +
                "(select demand_id,from_unixtime(cast(answer_time/1000000 as bigint),'yyyy-MM-dd HH:mm:ss') as answer_time, \n" +
                "out_id as sessionid,\n" +
                "called_number as mobile,\n" +
                "voice_url,\n" +
                "intention_results,\n" +
                "'out' as call_type, \n" +
                "create_time as create_time \n" +
                "from ods_call_sobot_task_detail where left(cast(create_time as string),10) >= '2022-07-19'\n" +
                "union all\n" +
                "select ifnull(demand_id,0) as demand_id,\n" +
                "cast(answer_time as string) as answer_time,\n" +
                "call_id as sessionid,\n" +
                "caller as mobile,\n" +
                "voice_url,\n" +
                "intention_name,\n" +
                "'in' as call_type,\n" +
                "create_time as create_time \n" +
                "from ods_call_in_sobot_detail where left(cast(create_time as string),10) >= '2022-07-19'\n" +
                ") call_table\n" +
                "left join\n" +
                "(select * from ods_driver_info) dr \n" +
                "on call_table.sessionid = dr.SessionId\n" +
                "left join\n" +
                "(select * from ods_driver_info_input) dri\n" +
                "on call_table.sessionid = dri.SessionId\n" +
                "left join\n" +
                "(select * from ods_price_log) pl\n" +
                "on call_table.sessionid = pl.sessionId \n" +
                "left join\n" +
                "(select session_id,collect(concat(notes ,':',intent)) as non_st from ods_non_standard_trans group by session_id) non_st\n" +
                "on call_table.sessionid = non_st.session_id");

        DataStream<Row> data1 = tableEnv.toRetractStream(tableQuery2, Row.class).filter(
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
        data1.addSink(new KuduSink1());
        exeEnv.execute("外呼司机基本信息&热标签&聊价_0719_外呼热标签信息");
    }

    public static class KuduSink1 extends RichSinkFunction<Row> {
        private KuduTable table = null;
        private KuduTable table1 = null;
        private KuduClient client = null;
        private KuduSession session = null;
        private String masteraddr = "172.27.16.41:7051,172.27.16.43:7051,172.27.16.49:7051";

        public void open(Configuration parameters) throws Exception {
            //创建kudu连接
            client = new KuduClient.KuduClientBuilder(masteraddr).defaultSocketReadTimeoutMs(6000).build();
            //打开表
            table = client.openTable("stage.stage_call_driver");
            table1 = client.openTable("stage.stage_call_driver_stat");
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
            String sessionid="";
            String demand_id="";
            String mobile = "";
            String record_url = "";
            String intention_results = "";
            String answer_time = "";
            String call_type = "";
            String isTeam = "0";
            String isStop = "0";
            String startCity = "";
            String endCity = "";
            String emptyDate = "";
            String carType = "";
            String carLength = "";
            String goods_name = "";
            String price = "";
            String jsonvalue = "";
            int isTempStop = 0;
            String create_time = "";

            try {
                if(null==value.getField(0)){
                    sessionid="";
                } else {
                    sessionid = value.getField(0).toString();
                }
                if(null==value.getField(1)){
                    demand_id="0";
                } else {
                    demand_id = value.getField(1).toString();
                }
                if (null==value.getField(2)){
                    mobile="";
                } else {
                    mobile = value.getField(2).toString();
                }

                if (null==value.getField(3)){
                    record_url="";
                } else {
                    record_url = value.getField(3).toString();
                }

                if (null==value.getField(4)){
                    intention_results="";
                } else {
                    intention_results = value.getField(4).toString();
                }

                if (null==value.getField(5)){
                    answer_time="";
                } else {
                    answer_time = value.getField(5).toString();
                }

                if (null==value.getField(6)){
                    call_type="";
                } else {
                    call_type = value.getField(6).toString();
                }

                if (null==value.getField(7)){
                    isTeam="0";
                } else {
                    isTeam = value.getField(7).toString();
                }

                if (null==value.getField(8)){
                    isStop="0";
                } else {
                    isStop = value.getField(8).toString();
                }

                if (null==value.getField(9)){
                    startCity="";
                } else {
                    startCity = value.getField(9).toString();
                }

                if (null==value.getField(10)){
                    endCity="";
                } else {
                    endCity = value.getField(10).toString();
                }

                if (null==value.getField(11)){
                    emptyDate="";
                } else {
                    emptyDate = value.getField(11).toString();
                }

                if (null==value.getField(12)){
                    carType="";
                } else {
                    carType = value.getField(12).toString();
                }

                if (null==value.getField(13)){
                    carLength="";
                } else {
                    carLength = value.getField(13).toString();
                }

                if (null==value.getField(14)){
                    goods_name="";
                } else {
                    goods_name = value.getField(14).toString();
                }

                if (null==value.getField(15)){
                    price="";
                } else {
                    price = value.getField(15).toString();
                }

                if (null == value.getField(16)) {
                    jsonvalue = "";
                } else {
                    jsonvalue = value.getField(16).toString();
                }
                if (null == value.getField(17)) {
                    isTempStop = 0;
                } else {
                    isTempStop = Integer.parseInt(value.getField(17).toString());
                }

                if (null == value.getField(18)) {
                    create_time = now_time;
                } else {
                    create_time = value.getField(18).toString();
                }


                JSONObject jsonObject_line_prefer = new JSONObject(new LinkedHashMap<>());
                String jsonvalue1 = jsonvalue.replaceAll("([=]\\d*| |\\{|\\})", "");
                JSONObject jsonObject = new JSONObject(new LinkedHashMap<>());
                JSONObject jsonObject1 = new JSONObject(new LinkedHashMap<>());
                if (jsonvalue1.length() > 0) {
                    for (String item : jsonvalue1.split(",")) {
                        jsonObject.put(item.split(":")[0], item.split(":")[1]);
                    }
                } else {
                    jsonObject.put("A", null);
                }

                session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
                session.setTimeoutMillis(60000);
                Upsert upsert = table.newUpsert();
                PartialRow row2 = upsert.getRow();
                row2.addString("sessionid", sessionid);
                row2.addString("demand_id", demand_id);
                row2.addString("mobile", mobile);
                row2.addString("record_url", record_url);
                row2.addString("intention_results", intention_results);
                row2.addString("answer_time", answer_time);
                row2.addString("call_type", call_type);
                row2.addString("isteam", isTeam);
                if (price.replace(" ", "").length() > 0) {
                    row2.addString("isstop", "0");
                } else if ((isStop.replace(" ", "")).length() > 0) {
                    row2.addString("isstop", isStop);
                }
                row2.addString("startcity", startCity);
                row2.addString("endcity", endCity);
                row2.addString("emptydate", emptyDate);
                row2.addString("cartype", carType);
                row2.addString("carlength", carLength);
                row2.addString("goods_name", goods_name);
                row2.addString("price", price);
                if (jsonObject.toJSONString().length() > 3) {
                    row2.addString("non_standard", jsonObject.toJSONString());
                }

                if (price.replace(" ", "").length() > 0) {
                    row2.addInt("is_tempstop", 0);
                } else if (isTempStop > 0) {
                    row2.addInt("is_tempstop", isTempStop);
                }

                String Utc_create_time = create_time;

                TimeZone utc = TimeZone.getTimeZone("UTC");
                if (create_time.contains("T")) {
                    SimpleDateFormat sourceFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                    SimpleDateFormat destFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    sourceFormat.setTimeZone(utc);
                    Date convertedDate = sourceFormat.parse(create_time);
                    Utc_create_time = destFormat.format(convertedDate);
//                    row2.addString("create_time", destFormat.format(convertedDate));
                } else {
                    Utc_create_time = create_time;
//                    row2.addString("create_time", create_time);
                }
                row2.addString("create_time", Utc_create_time);

//                    logger.info(row2.toString());
                session.apply(upsert);
                session.flush();

//                session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
//                session.setTimeoutMillis(60000);
                Upsert upsert1 = table1.newUpsert();
                PartialRow row22 = upsert1.getRow();
                row22.addString("mobile", mobile);
                if ((isTeam.replace(" ", "")).length() > 0) {
                    row22.addString("isteam", isTeam);
                }

                if (price.replace(" ", "").length() > 0) {
                    row22.addString("isstop", "0");
                } else if ((isStop.replace(" ", "")).length() > 0) {
                    row22.addString("isstop", isStop);
                }
                if ((startCity.replace(" ", "")).length() > 0) {
                    row22.addString("startcity", startCity);
                }
                if ((endCity.replace(" ", "")).length() > 0) {
                    row22.addString("endcity", endCity);
                }
                if ((emptyDate.replace(" ", "")).length() > 0) {
                    row22.addString("emptydate", emptyDate);
                }
                if ((carType.replace(" ", "")).length() > 0) {
                    row22.addString("cartype", carType);
                }
                if ((carLength.replace(" ", "")).length() > 0) {
                    row22.addString("carlength", carLength);
                }
                if ((goods_name.replace(" ", "")).length() > 0) {
                    row22.addString("goods_name", goods_name);
                }
                if ((price.replace(" ", "")).length() > 0) {
                    row22.addString("price", price);
                }

                if (price.replace(" ", "").length() > 0) {
                    row22.addInt("is_tempstop", 0);
                } else if (isTempStop > 0) {
                    row22.addInt("is_tempstop", isTempStop);
                }

                String jsonvalue11 = jsonvalue.replaceAll("([=]\\d*| |\\{|\\})", "");
                JSONObject jsonObject11 = new JSONObject(new LinkedHashMap<>());
                //这里先查询
                List<String> projectColumns = new ArrayList<String>();
                //添加select 字段名
                projectColumns.add("non_standard"); //字段名
                KuduScanner.KuduScannerBuilder builder = client.newScannerBuilder(table1).setProjectedColumnNames(projectColumns);
                KuduPredicate predicate1 = null;
                predicate1 = predicate1.newComparisonPredicate(table1.getSchema().getColumn("mobile"),
                        KuduPredicate.ComparisonOp.EQUAL, mobile);
                builder.addPredicate(predicate1);
                KuduScanner scanner = builder.build();
                JSONObject jsonvalue_old = new JSONObject(new LinkedHashMap<>());

                while (scanner.hasMoreRows()) {
                    RowResultIterator results = scanner.nextRows();
                    int numRows = results.getNumRows();
                    System.out.println("numRows count is : " + numRows);
                    while (results.hasNext()) {
                        RowResult result = results.next();
                        if (result.isNull(0)) {
                            break;
                        }
//                            jsonvalue_old = jsonObject;
                        String non_standards = result.getString(0);
                        jsonvalue_old = JSONObject.parseObject(non_standards);
                        if (jsonvalue11.length() > 3) {
                            for (String item : jsonvalue11.split(",")) {
//                                    System.out.println(jsonObject11.getString(item.split(":")[0]));
                                jsonvalue_old.put(item.split(":")[0], item.split(":")[1]);
                            }
                        } else {
                            jsonvalue_old.put("A", null);
                        }
                    }
                }
                System.out.println(jsonvalue_old.toJSONString());

                if ((jsonvalue_old.toJSONString().replace(" ", "")).length() > 3) {
                    row22.addString("non_standard", jsonvalue_old.toJSONString());
                }

                row22.addString("create_time", Utc_create_time);
                logger.info(row22.toString());
                session.apply(upsert1);
                session.flush();
            } catch (Exception e) {
                logger.error(e.getMessage(),e);
                e.printStackTrace();
            }
        }
    }
}
