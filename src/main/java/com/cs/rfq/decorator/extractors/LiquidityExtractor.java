package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.Map;

public class LiquidityExtractor {
    private Long since;
    private Dataset<Row> trades;

    public boolean checkLiquidity(DateTime dateTo,Long threshold,Rfq rfq,SparkSession session){

        long todayMs = dateTo.withMillisOfDay(0).getMillis();
        long since = dateTo.withMillis(todayMs).minusWeeks(1).getMillis();

        trades = new TradeDataLoader().loadTrades(session, "src/test/resources/trades/successful-trades.json");

        Dataset<Row> filtered = trades
                .filter(trades.col("SecurityId").equalTo(rfq.getIsin()));


        long tradesPastWeek = filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(since))).count();
        if(tradesPastWeek < threshold)
            return false;
        else
            return true;

    }
    
}
