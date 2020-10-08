package eu.dnetlib.iis.wf.importer.infospace.converter;

import com.google.common.collect.Lists;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.iis.wf.importer.infospace.approver.FieldApprover;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

import java.time.Year;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * {@link MetadataConverterUtils} test class. 
 * 
 * @author mhorst
 *
 */
public class MetadataConverterUtilsTest {

    private Logger log = mock(Logger.class);
    
    @Test
    public void testExtractYearOrNullWithValidInput() {
        assertEquals(Year.of(2020), MetadataConverterUtils.extractYearOrNull("2020-12-31", log));
        assertEquals(Year.of(2010), MetadataConverterUtils.extractYearOrNull("2010-1-1", log));
        assertEquals(Year.of(1900), MetadataConverterUtils.extractYearOrNull("1900-2", log));
        assertEquals(Year.of(2020), MetadataConverterUtils.extractYearOrNull("2020", log));
    }
    
    @Test
    public void testExtractYearOrNullWithInvalidInput() {
        assertNull(MetadataConverterUtils.extractYearOrNull("invalid", log));
        assertNull(MetadataConverterUtils.extractYearOrNull("20200304", log));
        assertNull(MetadataConverterUtils.extractYearOrNull("20-20-01", log));
        assertNull(MetadataConverterUtils.extractYearOrNull("", log));
        assertNull(MetadataConverterUtils.extractYearOrNull(null, log));
    }
    
    @Test
    public void testExtractValuesForNullApprover() {
        // given
        Collection<StructuredProperty> source = Lists.newArrayList();
        String validValue = "someValue";
        source.add(generateStructuredProperty(validValue, null));
        
        // execute
        assertThrows(NullPointerException.class, () -> MetadataConverterUtils.extractValues(source, null));
    }
    
    @Test
    public void testExtractValuesForNullSource() {
        assertThrows(NullPointerException.class, () ->
                MetadataConverterUtils.extractValues(null, mock(FieldApprover.class)));
    }
    
    @Test
    public void testExtractValuesForEmptySource() {
        //execute
        List<String> results = MetadataConverterUtils.extractValues(Lists.newArrayList(), mock(FieldApprover.class));
        
        //assert
        assertNotNull(results);
        assertTrue(results.isEmpty());
    }
    
    @Test
    public void testExtractValues() {
        // given
        FieldApprover notNullDataInfoFieldApprover = Objects::nonNull;
        DataInfo dataInfo = new DataInfo();
        Collection<StructuredProperty> source = Lists.newArrayList();
        String validValue = "someValue";
        source.add(generateStructuredProperty(validValue, dataInfo));
        source.add(generateStructuredProperty(null, dataInfo));
        source.add(generateStructuredProperty("", dataInfo));
        source.add(generateStructuredProperty("toBeRejectedBecauseOfNullDataInfo", null));
        
        // execute
        List<String> results = MetadataConverterUtils.extractValues(source, notNullDataInfoFieldApprover);
        
        // assert
        assertNotNull(results);
        assertEquals(1, results.size());
        assertEquals(validValue, results.get(0));
    }

    private static StructuredProperty generateStructuredProperty(String value, DataInfo dataInfo) {
        StructuredProperty structProp = new StructuredProperty();
        structProp.setValue(value);
        structProp.setDataInfo(dataInfo);
        return structProp;
    }
    
}
