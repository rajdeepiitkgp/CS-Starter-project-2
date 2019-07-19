package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.AbstractSparkUnitTest;
import com.cs.rfq.decorator.extractors.RfqMetadataExtractor;
import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.cs.rfq.decorator.extractors.UserTradeCount;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class UserTradeCountTest extends AbstractSparkUnitTest {

    @Test
    public void testUserTradeCount(){

        RfqMetadataExtractor r = new UserTradeCount();
        String validRfqJson = "{" +
                "'id': '123ABC', " +
                "'customerId' : 12040, "+
                "'traderId': 7514623710987345000, " +
                "'entityId': 5561279226039690843, " +
                "'instrumentId': 'AT0000383864', " +
                "'qty': 250000, " +
                "'price': 1.58, " +
                "'side': 'B' " +
                "}";
        Rfq rfq = new Rfq();
        rfq.fromJson(validRfqJson);
        Map<RfqMetadataFieldNames,Object> result;
        result = r.extractMetaData(rfq,session,null);
        double res = (Double)result.get(RfqMetadataFieldNames.userTradePercentage);
        assertEquals(62.93, res,0.01);



    }
}

