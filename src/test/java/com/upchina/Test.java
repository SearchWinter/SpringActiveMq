package com.upchina;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.List;

public class Test {
    public static void main(String[] args) {
        String str = null;
        String[] split = str.split("\\|", -1);
        System.out.println(split.length);

    }

    @org.junit.Test
    public void test(){
        String jsonString = "[\"a\",\"b\",\"c\"]";

        try {
            // 使用 Jackson 库解析 JSON 字符串为 List
            ObjectMapper objectMapper = new ObjectMapper();
            List<String> stringList = Arrays.asList(objectMapper.readValue(jsonString, String[].class));

            // 打印 List
            System.out.println(stringList);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    @org.junit.Test
    public void test3(){
        String str="cd_807ce734972a|d636a95ea8365351ec0f16a742dbea97|117|lc_xff|57|2|0";
        String[] split = str.split("\\|", -1);
        for (int i = 0; i <split.length ; i++) {
            System.out.println(split[i]);
        }
        int i = Integer.parseInt("0");
        System.out.println(i);
    }

    @org.junit.Test
    public void test4(){
        String str="{\"Author\":\"A0\",\"page\":0}";
        JSONObject jsonObject = new JSONObject(str);
        System.out.println("listenerReceiveMsg2：" + jsonObject.toString());
    }

}
