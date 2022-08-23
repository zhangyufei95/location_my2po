package com.example.cdc.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.apache.kudu.client.*;

import java.text.SimpleDateFormat;
import java.util.*;

public class Sink2Kudu extends RichSinkFunction<Row> {
    private KuduTable table = null;
    private KuduClient client = null;
    private KuduClient client1 = null;
    private KuduSession session = null;
    private String masteraddr = "172.27.16.41:7051,172.27.16.43:7051,172.27.16.49:7051";
    private String filed_str = "";
    private JSONObject js = new JSONObject();
    private String tableName = "";

    public void open(Configuration parameters) throws Exception {
        filed_str = ReadFile.SqlContent("sink_users.sql",2,"ht","sink");
        System.out.println(filed_str);
        js = JSONObject.parseObject(filed_str);
        tableName = js.getString("tableName");
        System.out.println(tableName);
        //创建kudu连接
        client = new KuduClient.KuduClientBuilder(masteraddr).defaultSocketReadTimeoutMs(6000).build();
        //打开表
        table = client.openTable(tableName);
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
        System.out.println(value.toString());
        Upsert upsert = table.newUpsert();
        PartialRow row = upsert.getRow();
//        System.out.println(js.getString("sink_str"));
        int cnt = 0;
        for (String entry : js.getString("sink_str").split("\\|")) {
            String key1 =  entry.split(" ")[0];
            String value1 = entry.split(" ")[1];
            if("int".contains(value1)){
                row.addInt(key1,Integer.parseInt(value.getField(cnt).toString()));
            } else if ("string".contains(value1)){
                row.addString(key1,value.getField(cnt).toString());
            }
            cnt = cnt+1;
//            System.out.println(row.toString());
        }
        session.apply(upsert);
        session.flush();
        }
    }
