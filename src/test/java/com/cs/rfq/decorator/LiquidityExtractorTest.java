package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.AbstractSparkUnitTest;
import com.cs.rfq.decorator.extractors.LiquidityExtractor;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LiquidityExtractorTest extends AbstractSparkUnitTest {

    @Test
    public void testLiquidityExtractorFalse() {

        String validRfqJson = "{" +
                "'id': '123ABC', " +
                "'customerId' : 12040, "+
                "'traderId': 7514623710987345000, " +
                "'entityId': 5561279226039690843, " +
                "'instrumentId': 'AT0000A0N9A0', " +
                "'qty': 250000, " +
                "'price': 1.58, " +
                "'side': 'B' " +
                "}";
        Rfq rfq = new Rfq();
        rfq.fromJson(validRfqJson);

        DateTime date = DateTime.now();

        Long thresold = 1L;

        LiquidityExtractor checkLiquidity = new LiquidityExtractor();

        boolean result = checkLiquidity.checkLiquidity(date,thresold,rfq,session);
        assertEquals(result,false);



    }

    @Test
    public void testLiquidityExtractorTrue() {

        String validRfqJson = "{" +
                "'id': '123ABC', " +
                "'customerId' : 12040, "+
                "'traderId': 7514623710987345000, " +
                "'entityId': 5561279226039690843, " +
                "'instrumentId': 'AT0000A0N9A0', " +
                "'qty': 250000, " +
                "'price': 1.58, " +
                "'side': 'B' " +
                "}";
        Rfq rfq = Rfq.fromJson(validRfqJson);


        DateTime date = DateTime.now();

        Long thresold = 1L;

        LiquidityExtractor checkLiquidity = new LiquidityExtractor();

        boolean result = checkLiquidity.checkLiquidity(date,thresold,rfq,session);
        assertEquals(result,true);



    }
}
