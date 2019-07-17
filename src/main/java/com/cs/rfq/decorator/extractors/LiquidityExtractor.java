package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.sql.Date;
import java.util.Map;

public class LiquidityExtractor {
    private Long since;
    private Dataset<Row> trades;

    public boolean checkLiquidity(DateTime dateTo, Long threshold, Rfq rfq, SparkSession session){

        long todayMs = dateTo.withMillisOfDay(0).getMillis();
        long since = dateTo.withMillis(todayMs).minusWeeks(1).getMillis();
        //long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
        //long pastWeekMs = DateTime.now().withMillis(todayMs).minusWeeks(1).getMillis();
        //long difference = todayMs - since;
        //System.out.println("difference " +difference);

        trades = new TradeDataLoader().loadTrades(session, "src/test/resources/trades/successful-trades.json");

        Dataset<Row> filtered = trades
                .filter(trades.col("SecurityId").equalTo(rfq.getIsin()));
        //System.out.println(filtered.count());
        //System.out.println("sec id "+rfq.getIsin());


        long tradesPastWeek = filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(since))).count();
        ////System.out.println(tradesPastWeek);
        if(tradesPastWeek < threshold)
            return false;
        else
            return true;

    }
    
}
