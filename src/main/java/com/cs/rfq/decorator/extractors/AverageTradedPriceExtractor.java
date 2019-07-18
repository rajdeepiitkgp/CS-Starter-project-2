package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.extractors.RfqMetadataExtractor;
import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;

import javax.xml.crypto.Data;
import java.util.HashMap;
import java.util.Map;

public class AverageTradedPriceExtractor  implements RfqMetadataExtractor {
    private String since;
    private String to;



    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {


        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();


        DateTime now = DateTime.now();
        DateTime monday = now.minusWeeks(1).withDayOfWeek(DateTimeConstants.MONDAY);
        DateTime sunday = monday.plusDays(6);
        setSince(monday.getYear() + "-" + (monday.getMonthOfYear() < 10 ? "0" + monday.getMonthOfYear() : monday.getMonthOfYear()) + "-" + (monday.getDayOfMonth() < 10 ? "0" + monday.getDayOfMonth() : monday.getDayOfMonth()));
        setTo(sunday.getYear() + "-" + (sunday.getMonthOfYear() < 10 ? "0" + sunday.getMonthOfYear() : sunday.getMonthOfYear()) + "-" + (sunday.getDayOfMonth() < 10 ? "0" + sunday.getDayOfMonth() : sunday.getDayOfMonth()));

      String query = String.format("SELECT sum(LastQty*LastPx)/sum(LastQty) from trade where  SecurityID='%s'  AND TradeDate >= '%s' AND TradeDate <= '%s'",

                rfq.getIsin(),
                since,
                to);
        System.out.println("Week: " + query);
        trades.createOrReplaceTempView("trade");
        Dataset<Row>sqlQueryResults = session.sql(query);

        Object volume = sqlQueryResults.first().get(0); System.out.println(volume);
        if (volume == null) {
            volume = 0L;
        }

        results.put(RfqMetadataFieldNames.AverageTradedPriceLastWeek, volume);
        return results;
    }

    protected void setSince(String since) {
        this.since = since;
    }

    public void setTo(String to) {
        this.to = to;
    }
}
