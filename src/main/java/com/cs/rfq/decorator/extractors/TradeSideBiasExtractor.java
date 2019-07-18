package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;

import java.util.HashMap;
import java.util.Map;

public class TradeSideBiasExtractor implements RfqMetadataExtractor {

    private String since;
    private String to;
    private Dataset<Row> successfulTrades;

    public TradeSideBiasExtractor() {
        this.since = DateTime.now().getYear() - 1 + "-01-01";
        this.to = DateTime.now().getYear() - 1 + "-12-31";
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        successfulTrades = new TradeDataLoader().loadTrades(session, "src/test/resources/trades/successful-trades.json");

        DateTime lastMonth = DateTime.now().minusMonths(1);
        setSince(lastMonth.getYear() + "-" + (lastMonth.getMonthOfYear() < 10 ? "0" + lastMonth.getMonthOfYear() : lastMonth.getMonthOfYear()) + "-01");
        setTo(lastMonth.getYear() + "-" + (lastMonth.getMonthOfYear() < 10 ? "0" + lastMonth.getMonthOfYear() : lastMonth.getMonthOfYear()) + "-" + new DateTime(DateTime.now().getYear(), DateTime.now().getMonthOfYear(), 1, 0, 0).minusDays(1).getDayOfMonth());

        String query = String.format("SELECT sum(LastQty) as amt from successfulTrades where CustomerID='%s' AND SecurityID='%s' AND TraderId = '%s' AND TradeDate >= '%s' AND TradeDate <= '%s' AND Side = '1'",
                rfq.getCustomerId(),
                rfq.getIsin(),
                rfq.getTraderId(),
                since,
                to);

        System.out.println("Month: " + query);
        successfulTrades.createOrReplaceTempView("successfulTrades");
        Dataset<Row> sqlQueryResults = session.sql(query);

        Object bought = sqlQueryResults.first().get(0);

        query = String.format("SELECT sum(LastQty) as amt from successfulTrades where CustomerID='%s' AND SecurityID='%s' AND TraderId = '%s' AND TradeDate >= '%s' AND TradeDate <= '%s' AND Side = '2'",
                rfq.getCustomerId(),
                rfq.getIsin(),
                rfq.getTraderId(),
                since,
                to);
        System.out.println("Month: " + query);
        sqlQueryResults = session.sql(query);

        Object sold = sqlQueryResults.first().get(0);

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();

        if (bought == null && sold == null) {
            results.put(RfqMetadataFieldNames.tradeBiasPastMonth, -1);
        } else if(sold == null) {
            results.put(RfqMetadataFieldNames.tradeBiasPastMonth, "No Selling. Only buying.");
        } else {
            results.put(RfqMetadataFieldNames.tradeBiasPastMonth, Float.parseFloat(bought.toString()) / Float.parseFloat(sold.toString()));
        }

        DateTime now = DateTime.now();
        DateTime monday = now.minusWeeks(1).withDayOfWeek(DateTimeConstants.MONDAY);
        DateTime sunday = monday.plusDays(6);
        setSince(monday.getYear() + "-" + (monday.getMonthOfYear() < 10 ? "0" + monday.getMonthOfYear() : monday.getMonthOfYear()) + "-" + (monday.getDayOfMonth() < 10 ? "0" + monday.getDayOfMonth() : monday.getDayOfMonth()));
        setTo(sunday.getYear() + "-" + (sunday.getMonthOfYear() < 10 ? "0" + sunday.getMonthOfYear() : sunday.getMonthOfYear()) + "-" + (sunday.getDayOfMonth() < 10 ? "0" + sunday.getDayOfMonth() : sunday.getDayOfMonth()));

        query = String.format("SELECT sum(LastQty) as amt from successfulTrades where CustomerID='%s' AND SecurityID='%s' AND TraderId = '%s' AND TradeDate >= '%s' AND TradeDate <= '%s' AND Side = '1'",
                rfq.getCustomerId(),
                rfq.getIsin(),
                rfq.getTraderId(),
                since,
                to);
        System.out.println("Week: " + query);
        successfulTrades.createOrReplaceTempView("successfulTrades");
        sqlQueryResults = session.sql(query);

        bought = sqlQueryResults.first().get(0);

        query = String.format("SELECT sum(LastQty) as amt from successfulTrades where CustomerID='%s' AND SecurityID='%s' AND TraderId = '%s' AND TradeDate >= '%s' AND TradeDate <= '%s' AND Side = '2'",
                rfq.getCustomerId(),
                rfq.getIsin(),
                rfq.getTraderId(),
                since,
                to);
        System.out.println("Week: " + query);
        sqlQueryResults = session.sql(query);
        sold = sqlQueryResults.first().get(0);

        //results = new HashMap<>();
        if (bought == null && sold == null) {
            results.put(RfqMetadataFieldNames.tradeBiasPastWeek, -1);
        } else if(sold == null) {
            results.put(RfqMetadataFieldNames.tradeBiasPastWeek, "No Selling. Only buying.");
        } else {
            results.put(RfqMetadataFieldNames.tradeBiasPastWeek,  Float.parseFloat(bought.toString()) / Float.parseFloat(sold.toString()));
        }

        return results;
    }

    protected void setSince(String since) {
        this.since = since;
    }

    public void setTo(String to) {
        this.to = to;
    }
}
