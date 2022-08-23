package com.example.cdc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.apache.kudu.client.*;

import java.text.SimpleDateFormat;
import java.util.*;

public class TestKudu  {
    public static void main(String[] args) throws Exception {

        String get_url = "https://gw.heptax.com/algo/recommend/get_city_belong";

        String msg = "{\"a\":{\"aa\":\"xx\"}}";

        String mm = "{\"aa\": \"{\"PhoneWork\": \"18675041094\", \"sentence\": \"喂，你好，那个昆山到平湖的货走了吗？\"}\", \"找货失败\":\"{\"PhoneWork\": \"18675041094\", \"sentence\": \"七米七的C。\"}\"}";

        String result ="{\"content\":\"{\\\"个人档案编号\\\":\\\"123123\\\",\\\"接种信息列表\\\":[{\\\"生产企业\\\":\\\"武汉生物\\\",\\\"剂次\\\":\\\"1\\\",\\\"接种日期\\\":\\\"2021-00-09\n" +
                "10:20:13\\\",\\\"疫苗名称\\\":\\\"新冠疫苗（Vero细胞）\\\",\\\"接种地\\\":\\\"上海市\\\"},{\\\"生产企业\\\":\\\"武汉生物\\\",\\\"剂次\\\":\\\"2\\\",\\\"接种日期\\\":\\\"2021-06-05\n" +
                "12:00:56\\\",\\\"疫苗名称\\\":\\\"新冠疫苗（Vero细胞）\\\",\\\"接种地\\\":\\\"上海市\\\"},{\\\"生产企业\\\":\\\"武汉生物\\\",\\\"剂次\\\":\\\"3\\\",\\\"接种日期\\\":\\\"2022-01-15\n" +
                "14:17:23\\\",\\\"疫苗名称\\\":\\\"新冠疫苗（Vero细胞）\\\",\\\"接种地\\\":\\\"上海市\\\"}],\\\"证件号码\\\":\\\"31022113951114571x\\\",\\\"个案状态\\\":\\\"正常\\\",\\\"国籍\\\":\\\"中国\\\",\\\"证件类型\\\":\\\"居民身份证\\\"}\",\"message\":\"success\"}";


        JSONObject jsonObject = JSONObject.parseObject(msg);
        System.out.println(jsonObject);
        String content = jsonObject.get("a").toString();
//        JSONObject jsonObject2 = (JSONObject) JSON.parse(content);
//        JSONArray arrayJson = jsonObject2.getJSONArray("接种信息列表");
        System.out.println(content);

        System.out.println(jsonObject);
        JSONObject jsonObject1 = JSONObject.parseObject("槽位循环");
        System.out.println(jsonObject1);
        System.exit(0);


        String call_in_content = "[{\"callID\":\"e8cff94f-1158-4327-9021-cf84bad2cb09\",\"dateTime\":1656330351238,\"senderType\":1,\"msg\":\"你好有什么可以帮助你\",\"nodeId\":\"c4fec0a8affc473884996488dd4d4d1d\"}]";

        String call_content = "[{\"callID\":\"91fda8f8-569e-4e74-96f4-4c7a4eb80841\",\"dateTime\":1656461248,\"senderType\":1,\"msg\":\"接口调用\",\"nodeName\":\"开场-推货\",\"remark\":\"\",\"nodeId\":\"c2dfc355c8ee45a1843312d233e7bc3e\"},{\"callID\":\"91fda8f8-569e-4e74-96f4-4c7a4eb80841\",\"dateTime\":0,\"senderType\":0,\"msg\":\"接口调用\",\"remark\":\"匹配到：【开场-推货 #接口成功 】\"},{\"callID\":\"91fda8f8-569e-4e74-96f4-4c7a4eb80841\",\"dateTime\":1656461248,\"senderType\":1,\"msg\":\"有个长沙到湘西土家族苗族自治州的货，你今天在不在长沙附近啊？\",\"nodeName\":\"开场话术节点\",\"remark\":\"匹配到：【开场-推货 #接口成功 】\",\"nodeId\":\"8d9c2af3dcf047f29a4ace487f237dae\"},{\"callID\":\"91fda8f8-569e-4e74-96f4-4c7a4eb80841\",\"dateTime\":1656461248,\"senderType\":0,\"msg\":\"不在不在。\",\"remark\":\"匹配到：【开场话术节点#内容输出校验】\"},{\"callID\":\"91fda8f8-569e-4e74-96f4-4c7a4eb80841\",\"dateTime\":1656461259,\"senderType\":1,\"msg\":\"接口调用\",\"nodeName\":\"意图识别-rasa\",\"remark\":\"匹配到：【开场话术节点#内容输出校验】\",\"nodeId\":\"3ed7d466c4f240959cd9ed1c46364c4b\"},{\"callID\":\"91fda8f8-569e-4e74-96f4-4c7a4eb80841\",\"dateTime\":1656461248,\"senderType\":0,\"msg\":\"接口调用成功\",\"remark\":\"匹配到：【意图识别-rasa #接口否定-0 】\"},{\"callID\":\"91fda8f8-569e-4e74-96f4-4c7a4eb80841\",\"dateTime\":1656461259,\"senderType\":1,\"msg\":\"接口调用\",\"nodeName\":\"时空判断\",\"remark\":\"匹配到：【意图识别-rasa #接口否定-0 】\",\"nodeId\":\"fed0bdd9753d4464880a93dc403615e5\"},{\"callID\":\"91fda8f8-569e-4e74-96f4-4c7a4eb80841\",\"dateTime\":1656461248,\"senderType\":0,\"msg\":\"接口调用\",\"remark\":\"匹配到：【时空判断 #接口时空不符-无槽位 】\"},{\"callID\":\"91fda8f8-569e-4e74-96f4-4c7a4eb80841\",\"dateTime\":1656461259,\"senderType\":1,\"msg\":\"接口调用\",\"nodeName\":\"rasa\",\"remark\":\"匹配到：【时空判断 #接口时空不符-无槽位 】\",\"nodeId\":\"305acc95052a4b5bb05b9a587c04c6c8\"},{\"callID\":\"91fda8f8-569e-4e74-96f4-4c7a4eb80841\",\"dateTime\":1656461248,\"senderType\":0,\"msg\":\"接口调用\",\"remark\":\"匹配到： 【rasa#接口失败#】\"},{\"callID\":\"91fda8f8-569e-4e74-96f4-4c7a4eb80841\",\"dateTime\":1656461259,\"senderType\":1,\"msg\":\"\",\"nodeName\":\"rasa\",\"remark\":\"匹配到： 【rasa#接口失败#】\",\"nodeId\":\"305acc95052a4b5bb05b9a587c04c6c8\"},{\"callID\":\"91fda8f8-569e-4e74-96f4-4c7a4eb80841\",\"dateTime\":1656461259,\"senderType\":0,\"msg\":\"我在山东呢。\",\"remark\":\"匹配到： 【rasa#接口失败#】\"},{\"callID\":\"91fda8f8-569e-4e74-96f4-4c7a4eb80841\",\"dateTime\":1656461263,\"senderType\":1,\"msg\":\"\",\"nodeName\":\"rasa\",\"remark\":\"匹配到： 【rasa#接口失败#】\",\"nodeId\":\"305acc95052a4b5bb05b9a587c04c6c8\"}]\n";
        JSONArray jsonArray = JSONArray.parseArray(call_content);
        JSONArray jsonArray1 = jsonArraySort(jsonArray,"dateTime",true);
        System.out.println(jsonArray1);
        List<JSONObject> list1 = JSONArray.parseArray(jsonArray1.toJSONString(), JSONObject.class);
        String content_str = "";
        for (JSONObject xx:list1) {
            String msg_str = xx.getString("msg").replace(" ","");
            System.out.println();
            if (!("接口调用".equals(xx.getString("msg"))) && !("接口调用成功".equals(xx.getString("msg"))) && !("".equals(msg_str))) {
                if(xx.getString("senderType").equals("1")){
                    content_str=content_str+"超级调度: "+msg_str+"\n";
                } else if(xx.getString("senderType").equals("0")){
                    content_str=content_str+"司机:    "+msg_str+"\n";
                }
            }
        }
        System.out.println(content_str);
    }

    public static JSONArray jsonArraySort(JSONArray array, final String sortColName,
                                          final boolean isAsc) {
        JSONArray sortedJsonArray = new JSONArray();
        List<JSONObject> jsonList = new ArrayList<JSONObject>();
        for (int i = 0; i < array.size(); i++) {
            jsonList.add(array.getJSONObject(i));
        }

        //将 JSONArray 转换成 List,本质上还是对 List 进行排序
        Collections.sort(jsonList, new Comparator<JSONObject>() {
            @Override
            public int compare(JSONObject o1, JSONObject o2) {
                String valA = o1.getString(sortColName);
                String valB = o2.getString(sortColName);
                if (isAsc) {
                    // 升序
                    return valA.compareTo(valB);
                } else {
                    return valB.compareTo(valA);
                }
            }
        });
        // 此时jsonList已经是排完序的集合了
        for(JSONObject obj : jsonList) {
            sortedJsonArray.add(obj);
        }
        return sortedJsonArray;
    }
}
