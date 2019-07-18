package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TradeSideBiasExtrctorTest extends AbstractSparkUnitTest{
    private Rfq rfq;

    @Before
    public void setup() {
        rfq = new Rfq();
        rfq.setId("732");
        rfq.setEntityId(5561279226039690843L);
        rfq.setIsin("AT0000A10683");
        rfq.setCustomerId(12002L );
        rfq.setTraderId(7514623710987345030L);
        rfq.setPrice(121.99);
        rfq.setSide("2");
    }


    @Test
    public void checkRatioWhenAllTradesMatch() {

        //String filePath = getClass().getResource("all-trades.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, "src/test/resources/trades/all-trades.json");

        TradeSideBiasExtractor extractor = new TradeSideBiasExtractor();


        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.tradeBiasPastMonth);

        assertEquals(2.0F, result);

        Object result1 = meta.get(RfqMetadataFieldNames.tradeBiasPastWeek);

        assertEquals(0.5F, result1);


    }
}
