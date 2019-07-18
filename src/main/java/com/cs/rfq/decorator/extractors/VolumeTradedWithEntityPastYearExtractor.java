package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateMidnight;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;

import java.util.HashMap;
import java.util.Map;

public class VolumeTradedWithEntityPastYearExtractor implements RfqMetadataExtractor {

    private String since;
    private String to;

    public VolumeTradedWithEntityPastYearExtractor() {
        this.since = DateTime.now().getYear() - 1 + "-01-01";
        this.to = DateTime.now().getYear() - 1 + "-12-31";
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        String query = String.format("SELECT sum(LastQty) from trade where CustomerID='%s' AND SecurityID='%s' AND TradeDate >= '%s' AND TradeDate <= '%s'",
                rfq.getCustomerId(),
                rfq.getIsin(),
                since,
                to);
        System.out.println("Year:" + query);
        trades.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResults = session.sql(query);

        Object volume = sqlQueryResults.first().get(0);
        if (volume == null) {
            volume = 0L;
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.volumeTradedPastYearForThisEntity, volume);

        DateTime lastMonth = DateTime.now().minusMonths(1);
        setSince(lastMonth.getYear() + "-" + (lastMonth.getMonthOfYear() < 10 ? "0" + lastMonth.getMonthOfYear() : lastMonth.getMonthOfYear()) + "-01");
        setTo(lastMonth.getYear() + "-" + (lastMonth.getMonthOfYear() < 10 ? "0" + lastMonth.getMonthOfYear() : lastMonth.getMonthOfYear()) + "-" + new DateTime(DateTime.now().getYear(), DateTime.now().getMonthOfYear(), 1, 0, 0).minusDays(1).getDayOfMonth());

        query = String.format("SELECT sum(LastQty) from trade where CustomerID='%s' AND SecurityID='%s' AND TradeDate >= '%s' AND TradeDate <= '%s'",
                rfq.getCustomerId(),
                rfq.getIsin(),
                since,
                to);
        System.out.println("Month: " + query);
        trades.createOrReplaceTempView("trade");
        sqlQueryResults = session.sql(query);

        volume = sqlQueryResults.first().get(0);
        if (volume == null) {
            volume = 0L;
        }

        DateTime now = DateTime.now();
        DateTime monday = now.minusWeeks(1).withDayOfWeek(DateTimeConstants.MONDAY);
        DateTime sunday = monday.plusDays(6);
        setSince(monday.getYear() + "-" + (monday.getMonthOfYear() < 10 ? "0" + monday.getMonthOfYear() : monday.getMonthOfYear()) + "-" + (monday.getDayOfMonth() < 10 ? "0" + monday.getDayOfMonth() : monday.getDayOfMonth()));
        setTo(sunday.getYear() + "-" + (sunday.getMonthOfYear() < 10 ? "0" + sunday.getMonthOfYear() : sunday.getMonthOfYear()) + "-" + (sunday.getDayOfMonth() < 10 ? "0" + sunday.getDayOfMonth() : sunday.getDayOfMonth()));

        query = String.format("SELECT sum(LastQty) from trade where CustomerID='%s' AND TraderId=SecurityID='%s' AND TradeDate >= '%s' AND TradeDate <= '%s'",
                rfq.getCustomerId(),
                rfq.getIsin(),
                since,
                to);
        System.out.println("Week: " + query);
        trades.createOrReplaceTempView("trade");
        sqlQueryResults = session.sql(query);

        volume = sqlQueryResults.first().get(0);
        if (volume == null) {
            volume = 0L;
        }

        results.put(RfqMetadataFieldNames.volumeTradedPastWeekForThisEntity, volume);
        return results;
    }

    protected void setSince(String since) {
        this.since = since;
    }

    public void setTo(String to) {
        this.to = to;
    }
}
