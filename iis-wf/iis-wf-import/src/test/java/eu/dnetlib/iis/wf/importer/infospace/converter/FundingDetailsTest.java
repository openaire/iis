package eu.dnetlib.iis.wf.importer.infospace.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;

import org.junit.Test;

/**
 * @author mhorst
 *
 */
public class FundingDetailsTest {

    @Test
    public void testBuildFundingClass() {
        assertNull(new FundingDetails(null, null).buildFundingClass());
        assertNull(new FundingDetails(null, Arrays.asList(new CharSequence[]{})).buildFundingClass());
        assertNull(new FundingDetails(null, Arrays.asList(new CharSequence[]{null})).buildFundingClass());
        assertEquals("EC::", new FundingDetails("EC", Arrays.asList(new CharSequence[]{})).buildFundingClass());
        assertEquals("EC::", new FundingDetails("EC", null).buildFundingClass());
        assertEquals("EC::name1", new FundingDetails("EC", Arrays.asList(new CharSequence[]{"name1", "name2"})).buildFundingClass());
        assertEquals("::name1", new FundingDetails(null, Arrays.asList(new CharSequence[]{"name1", "name2"})).buildFundingClass());
    }

}
