package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class UserTradeCount implements RfqMetadataExtractor {
    private Dataset<Row> trades_common;


    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        //trades will have data from all-trades.json
        trades = new TradeDataLoader().loadTrades(session, "src/test/resources/trades/all-trades.json");
        Dataset<Row> filtered = trades
                .filter(trades.col("CustomerID").equalTo(rfq.getCustomerId()));


        trades_common = new TradeDataLoader().loadTrades(session, "src/test/resources/trades/successful-trades.json");

                Dataset<Row> filtered_common = trades_common
                .filter(trades_common.col("CustomerId").equalTo(rfq.getCustomerId()))
                .filter(trades_common.col("TraderId").equalTo(rfq.getTraderId()));

               double percentage = (trades_common.count()*100.0/trades.count());
               Map<RfqMetadataFieldNames,Object> result = new HashMap();
               result.put(RfqMetadataFieldNames.userTradePercentage,percentage);
               System.out.println("Denominator : " + trades.count());

                System.out.println("Percentage interest = "+(trades_common.count()*100.0/trades.count()));

        return result;
    }
}
