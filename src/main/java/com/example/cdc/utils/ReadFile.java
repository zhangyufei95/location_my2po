package com.example.cdc.utils;

import com.alibaba.fastjson.JSONObject;
import jdk.nashorn.internal.objects.AccessorPropertyDescriptor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ReadFile {
    //字符编码
    private static String ENCODING = "utf-8";
    public static void main(String[] args) throws Exception {
        SqlContent("source.sql",1,"ht","source");
    }
    //tag:1:表示mysql-cdc表,2:表示jdbc方式，维表
    public static String SqlContent(String file_name,int tag,String dataBaseLevel,String model) throws Exception {
        InputStreamReader read = null;
        //这个 ReadFile 换成其他包中的其他类也可以，就比如换成 UserController 也是一样的
        String filePath = com.example.cdc.utils.ReadFile.class.getClassLoader().getResource(file_name).getPath();
        /*	通过类加载器获取资源文件的URL
         *	URL url = getClass().getClassLoader().getResource("my.txt");
         *	String filePath = url.getPath();
         *	其实这个和上面那个方法是一样的，因为：ReadFile.class.getClassLoader().getResource("my.txt")的返回值就是URL。
         */

        File file = new File(filePath);

        try {
            read = new InputStreamReader(new FileInputStream(file),ENCODING);
            //文件流是否存在
            if(file.isFile() && file.exists()) {
                @SuppressWarnings("resource")
                BufferedReader bufferedReader = new BufferedReader(read);
                String txt = null;
                String dataBase = "";
                String tableName = "";
                String filedList = "";
                String sql_str = "";
                String sink_str = "";
                Map<String,String> map_sink = new HashMap<>();
                JSONObject jsonObject = new JSONObject(new LinkedHashMap<>());
                while ((txt = bufferedReader.readLine()) != null) {
                    txt = txt.replaceAll(" +", " ");
                    if ("sink".equals(model)) {
                        if ((txt.contains('.'+"")) && (txt.contains("create"))) {
                            jsonObject.put("tableName",txt.split(" ")[2]);
                        } else if ((!txt.contains('.' + "")) && (txt.contains("comment"))) {
                            String key1 = txt.split(" ")[1];
                            String value1 = txt.split(" ")[2];
                            sink_str = sink_str+key1+" "+value1+"|";
                            jsonObject.put(key1,value1);
                        }
                    } else if ("query".equals(model)) {
                        sql_str = sql_str + txt;
                    } else {
                        if ((txt.contains('.' + "")) && (txt.contains("CREATE"))) {
                            dataBase = txt.split(" ")[2].replaceAll("\\`", "").split("\\.")[0];
                            tableName = txt.split(" ")[2].replaceAll("\\`", "").split("\\.")[1];
                        } else {
                            filedList = filedList + txt.split(" ")[1]; //.replaceAll("\\`", "");
                            String typeName = txt.split(" ")[2].replaceAll("\\`", "").split("\\(")[0].replaceAll("varchar", "string").replaceAll("datetime","timestamp").replaceAll("decimal", "double").replaceAll("longtext","string").replaceAll("mediumint","int");
                            filedList = filedList + " " + typeName + ",";
                        }
                    }
                }

                if ("sink".equals(model)) {
                    jsonObject.put("sink_str",sink_str);
                    sql_str = jsonObject.toJSONString();
                } else if ("source".equals(model)) {
                    filedList = filedList.split("PRIMARY")[0];
                    filedList = filedList.substring(0, filedList.length() - 1);
                    Map<String, String> map = new HashMap<>();
                    map.put("ht", "172.27.0.48|flink_cdc_ad|jklsek52@=9H");
                    map.put("algo", "172.27.16.38|root|Zhicang@2017");
                    map.put("pg","172.27.0.119|algo_pgm|klsy8#$1qsA7");
                    String hostName = map.get(dataBaseLevel).split("\\|")[0];
                    String serverId = String.valueOf(Math.round(Math.random() * (500000 - 100000) + 100000));
                    String userName = map.get(dataBaseLevel).split("\\|")[1];
                    String passWord = map.get(dataBaseLevel).split("\\|")[2];
                    if (tag == 1) {
                        sql_str = "CREATE TABLE IF NOT EXISTS ods_" + tableName + " ( " + filedList + " ) with(" +
                                " 'connector' = 'mysql-cdc', " +
                                " 'hostname' = '" + hostName + "', " +
                                " 'server-id' = '" + serverId + "', " +
                                "  'port' = '3306', " +
                                "  'username' = '" + userName + "', " +
                                "  'password' = '" + passWord + "', " +
                                "  'database-name' = '" + dataBase + "', " +
                                "  'table-name' = '" + tableName + "', " +
                                "  'server-time-zone' = 'Asia/Shanghai', " +
                                "  'debezium.snapshot.locking.mode' = 'none', " +
                                "  'scan.incremental.snapshot.enabled' = 'false', " +
                                "  'debezium.snapshot.mode' = 'initial' " +
                                ")";
                    } else {
                        sql_str = "CREATE TABLE IF NOT EXISTS  ods_" + tableName + " ( " + filedList + ",proc_time AS PROCTIME() ) with(" +
                                " 'connector' = 'jdbc', " +
                                "'driver'='com.mysql.cj.jdbc.Driver'," +
                                "  'username' = '" + userName + "', " +
                                "  'password' = '" + passWord + "', " +
                                "  'table-name' = '" + tableName + "', " +
                                " 'url' = 'jdbc:mysql://" + hostName + ":3306/" + dataBase + "'," +
                                " 'lookup.cache.max-rows' = '1000'," +
                                " 'lookup.cache.ttl' = '10000'" +
                                ")";
                    }

                    if("pg".equals(dataBaseLevel)){
                        sql_str = "CREATE TABLE IF NOT EXISTS  ods_" + tableName + " ( " + filedList + " ) with(" +
                                " 'connector' = 'postgres-cdc',\n" +
                                " 'hostname' = '"+ hostName +"',\n" +
                                " 'port' = '5432',\n" +
                                " 'username' = '" +userName+ "',\n" +
                                " 'password' = '"+passWord + "',\n" +
                                " 'database-name' = '" +dataBase + "',\n" +
                                " 'schema-name' = 'public',\n" +
                                " 'debezium.snapshot.mode' = 'initial',\n" +
                                " 'decoding.plugin.name' = 'pgoutput',\n" +
                                " 'debezium.slot.name' = 'ht_caller0713',\n" +
                                " 'table-name' = '" + tableName + "'\n" +
                                ")";
                    }
                }
                return sql_str;
            }else{
                throw new Exception("文件不存在");
            }
        } catch (Exception e) {
            throw e;
        }finally{
            //关闭文件流
            read.close();
        }
    }
}

