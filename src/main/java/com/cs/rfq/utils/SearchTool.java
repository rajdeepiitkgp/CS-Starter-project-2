package com.cs.rfq.utils;

import com.cs.rfq.decorator.Rfq;
import jdk.nashorn.internal.parser.JSONParser;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.function.IntBinaryOperator;


public class SearchTool  {

    private int option;
    public static String file_path = "src/test/resources/trades/historic-trades.json";
    private long customerId;
    private Dataset<Row> history;
    //protected static SparkSession session;
    public Map<Integer, String> showHistory(long custid){
        //Map<int,Rfq> rfList = new HashMap<Object, Object>()
        Map<Integer, String> rfqList = new HashMap<>();
        int num =1;
        try {
            File file = new File(file_path);
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {

                Rfq temp = new Rfq().fromJson(line);
                if(temp.getCustomerId()==custid)
                {
                    System.out.println("Press "+num+" for "+ line);
                    rfqList.put(num,line);
                    num++;
                }

            }
            fileReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


        return rfqList;

    }

    public long getCustomerId() {
        return customerId;
    }

    public void setCustomerId(long customerId) {
        this.customerId = customerId;
    }

    public void addHistory(String message){



        try{
            FileWriter fw=new FileWriter(file_path,true);
            fw.write(message);
            if(message!="")
                fw.write('\n');
            fw.close();
        }catch(Exception e){System.out.println(e);}
    }

    public int getOption() {
        return option;
    }

    public void setOption(int option) {
        this.option = option;
    }
}
