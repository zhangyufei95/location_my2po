package com.example.cdc;

import com.alibaba.fastjson.JSONObject;
import com.example.cdc.utils.ReadFile;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Test {

    public static void main(String[] args) throws Exception {
        String filed_str = ReadFile.SqlContent("sink_users.sql",2,"ht","sink");
        System.out.println(filed_str);
        System.exit(0);


        System.out.println(ReadFile.SqlContent("source_users.sql",1,"ht","source"));
        System.exit(0);

        String[] tmp = {"A","B","D","C","A","D"};

            //实例化一个set集合
        Set set = new HashSet();
        //遍历数组并存入集合,如果元素已存在则不会重复存入
        for (int i = 0; i < tmp.length; i++) {
            set.add(tmp[i]);
        }
        //返回Set集合的数组形式
        List<String> list = new ArrayList<String>(set);
        System.out.println(list);

        System.exit(0);
        String json_str = "{\"ns_001\":\"需要盖雨布\"}";
//        String json_str = "{\"ns_001\":\"需要盖雨布\",\"ns_002\":\"需要上叉车\",\"ns_003\":\"需要司机帮装\",\"ns_017\":\"需要排队\",\"ns_004\":\"需等配货\",\"ns_018\":\"驾驶员不准下车\",\"ns_005\":\"底板要平整\",\"ns_006\":\"需要枕木\",\"ns_007\":\"需要垫薄膜\",\"ns_019\":\"要有灭火器\",\"ns_008\":\"高栏全拆立柱\",\"ns_009\":\"高栏半拆立柱\",\"ns_010\":\"国五车及以上\",\"ns_011\":\"全程走高速\",\"ns_020\":\"装货地禁区\",\"ns_021\":\"卸货地禁区\",\"ns_022\":\"需48小时核酸\",\"ns_023\":\"需24小时核酸\",\"ns_024\":\"需通行证\"}";
        JSONObject jsonObject = new JSONObject();
        jsonObject=JSONObject.parseObject(json_str);
        String xx= jsonObject.toJSONString();
        String xxx = xx.replaceAll("([{]|[}]|[\"])", "");
        String[] tmp1 = xxx.split(",");
        System.out.println(tmp1[0]);
        System.out.println(tmp1[0].split(":")[1]);

//        System.out.println(tmp[-1]);
        System.out.println(jsonObject.getString("ns_001"));
        System.exit(0);

        String source_str = "{0:1=1, 1:1=2, 2:1=1}";
        String aa = "";
        String regEx = "(=1|=2| |\\{|\\})";

        String pattern = "(\\=D*)(\\=d+)(.*)";

//        var jsonStr =jsonStr.replace(/\d+/g,"100");


        String intention_city_id1 = source_str.replaceAll("([=]\\d*| |\\{|\\})", aa);


        for (String item : intention_city_id1.split(",")){
            jsonObject.put(item.split(":")[0],item.split(":")[1]);
        }
        System.out.println(jsonObject);
//        String intention_city_cluster1 = intention_city_cluster.replaceAll(regEx, aa);
//        System.out.println(intention_city_id1);
    }
}
