package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class OverallVolumeTradedWithEntityPastYearExtractorTest extends AbstractSparkUnitTest{

    private Rfq rfq;

    @Before
    public void setup() {
        rfq = new Rfq();
        rfq.setId("732");
        rfq.setEntityId(5561279226039690843L);
        rfq.setIsin("AT0000A10683");
        rfq.setCustomerId(14000L );
        rfq.setTraderId(7514623710987345031L);
        rfq.setPrice(121.99);
        rfq.setSide("2");
    }


    @Test
    public void checkEnityTradesWhenAllTradesMatch() {

        //String filePath = getClass().getResource("all-trades.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, "src/test/resources/trades/all-trades.json");

        OverallVolumeTradedWithEntityPastYearExtractor extractor = new OverallVolumeTradedWithEntityPastYearExtractor();


        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.overallVolumeTradedPastWeek);

        assertEquals(350000L, result);

        Object result1 = meta.get(RfqMetadataFieldNames.overallVolumeTradedPastMonth);

        assertEquals(3500000L, result1);

        Object result2 = meta.get(RfqMetadataFieldNames.overallVolumeTradedPastYear);

        assertEquals(5550000L, result2);

    }


}