package eu.dnetlib.iis.wf.importer.infospace.converter;

import com.google.common.collect.Lists;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Instance;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.iis.wf.importer.infospace.approver.FieldApprover;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Year;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
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

    @Nested
    public class ExtractPidTest {

        @Test
        @DisplayName("Empty pid list is returned for result with null instance list")
        public void givenAResultWithNullInstanceList_whenPidListIsExtracted_thenEmptyCollectionIsReturned() {
            Result result = new Result();

            List<StructuredProperty> pidList = MetadataConverterUtils.extractPid(result);

            assertThat(pidList, empty());
        }

        @Test
        @DisplayName("Empty pid list is returned for result with empty instance list")
        public void givenAResultWithEmptyInstanceList_whenPidListIsExtracted_thenEmptyCollectionIsReturned() {
            Result result = new Result();
            result.setInstance(Collections.emptyList());

            List<StructuredProperty> pidList = MetadataConverterUtils.extractPid(result);

            assertThat(pidList, empty());
        }

        @Test
        @DisplayName("Empty pid list is returned for result with instance list of element without pid list and without alternate identifier list")
        public void givenAResultWithInstanceListOfElementWithoutPidListAndAlternateIdentifierList_whenPidListIsExtracted_thenEmptyCollectionIsReturned() {
            Result result = new Result();
            result.setInstance(Collections.singletonList(new Instance()));

            List<StructuredProperty> pidList = MetadataConverterUtils.extractPid(result);

            assertThat(pidList, empty());
        }

        @Test
        @DisplayName("Empty pid list is returned for result with instance list of element with empty pid list and empty alternate identifier list")
        public void givenAResultWithInstanceListOfElementWithEmptyPidListAndEmptyAlternateIdentifierList_whenPidListIsExtracted_thenEmptyCollectionIsReturned() {
            Instance instance = new Instance();
            instance.setPid(Collections.emptyList());
            instance.setAlternateIdentifier(Collections.emptyList());
            Result result = new Result();
            result.setInstance(Collections.singletonList(instance));

            List<StructuredProperty> pidList = MetadataConverterUtils.extractPid(result);

            assertThat(pidList, empty());
        }

        @Test
        @DisplayName("Pid list is returned for instance list of element with pid list")
        public void givenAResultWithInstanceOfElementWithPidList_whenPidListIsExtracted_thenPidListIsReturned() {
            StructuredProperty pid = mock(StructuredProperty.class);
            Instance instance = new Instance();
            instance.setPid(Collections.singletonList(pid));
            Result result = new Result();
            result.setInstance(Collections.singletonList(instance));

            List<StructuredProperty> pidList = MetadataConverterUtils.extractPid(result);

            assertThat(pidList.size(), equalTo(1));
            assertThat(pidList, hasItem(pid));
        }

        @Test
        @DisplayName("Pid list is returned for instance list of element without pid list and with alternate identifier list")
        public void givenAResultWithInstanceOfElementWithoutPidListAndWithAlternateIdentifierList_whenPidListIsExtracted_thenAlternateIdentifierListIsReturned() {
            StructuredProperty pid = mock(StructuredProperty.class);
            Instance instance = new Instance();
            instance.setAlternateIdentifier(Collections.singletonList(pid));
            Result result = new Result();
            result.setInstance(Collections.singletonList(instance));

            List<StructuredProperty> pidList = MetadataConverterUtils.extractPid(result);

            assertThat(pidList.size(), equalTo(1));
            assertThat(pidList, hasItem(pid));
        }
    }
}
