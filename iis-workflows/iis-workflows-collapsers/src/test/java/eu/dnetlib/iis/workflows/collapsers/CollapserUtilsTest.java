package eu.dnetlib.iis.workflows.collapsers;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.IndexedRecord;

import static org.junit.Assert.*;

import org.junit.Test;
import org.python.google.common.collect.Lists;

import eu.dnetlib.iis.workflows.collapsers.CollapserUtils;

/**
 * 
 * @author Dominika Tkaczyk
 *
 */
public class CollapserUtilsTest {

    @Test
    public void testHaveEqualSchema() {
        assertTrue(CollapserUtils.haveEqualSchema(null));
        assertTrue(CollapserUtils.haveEqualSchema(new ArrayList<IndexedRecord>()));
        assertTrue(CollapserUtils.haveEqualSchema(
                Lists.newArrayList((IndexedRecord)SampleData.envMetadataRecord11, SampleData.envMetadataRecord12)));
        assertFalse(CollapserUtils.haveEqualSchema(
                Lists.newArrayList((IndexedRecord)SampleData.envMetadataRecord11, SampleData.envMetadataRecord12, SampleData.envTextRecord)));
    }
    
    @Test
    public void testIsOriginSchema() {
        assertTrue(CollapserUtils.isEnvelopeSchema(SampleData.envMetadataRecord11.getSchema()));
        assertFalse(CollapserUtils.isEnvelopeSchema(SampleData.textRecord.getSchema()));
    }
    
    @Test
    public void testGetOriginValue() {
        assertEquals("origin1", CollapserUtils.getOriginValue(SampleData.envMetadataRecord11));
        assertEquals("origin1", CollapserUtils.getOriginValue(SampleData.envTextRecord));
    }
    
    @Test
    public void testGetDataRecord() {
        SampleData.assertEqualRecords(
                SampleData.metadataRecord11, 
                CollapserUtils.getDataRecord(SampleData.envMetadataRecord11));
        SampleData.assertEqualRecords(
                SampleData.textRecord, 
                CollapserUtils.getDataRecord(SampleData.envTextRecord));
    }
        
    @Test
    public void testGetNumberOfFilledFields() {
        assertEquals(8, CollapserUtils.getNumberOfFilledFields(SampleData.metadataRecord11, null));
        assertEquals(2, CollapserUtils.getNumberOfFilledFields(SampleData.metadataRecord11, SampleData.significantFields));
    }
    
    @Test
    public void testSortByFilledFields() {
        List<IndexedRecord> empty = new ArrayList<IndexedRecord>();
        CollapserUtils.sortByFilledDataFields(empty, SampleData.significantFields);
        assertTrue(empty.isEmpty());
        
        List<IndexedRecord> oneElement = Lists.newArrayList((IndexedRecord)SampleData.metadataRecord13);
        CollapserUtils.sortByFilledDataFields(oneElement, SampleData.significantFields);
        assertEquals(Lists.newArrayList(SampleData.metadataRecord13),
                oneElement);
        
        List<IndexedRecord> list = Lists.newArrayList(
                (IndexedRecord)SampleData.metadataRecord11, SampleData.metadataRecord12, SampleData.metadataRecord13);
        CollapserUtils.sortByFilledDataFields(list, SampleData.significantFields);
        assertEquals(Lists.newArrayList(SampleData.metadataRecord12, SampleData.metadataRecord11, SampleData.metadataRecord13),
                list);
       
        CollapserUtils.sortByFilledDataFields(list, null);
        assertEquals(Lists.newArrayList(SampleData.metadataRecord11, SampleData.metadataRecord12, SampleData.metadataRecord13),
                list);
    }
    
    @Test
    public void testMerge() {
        SampleData.assertEqualRecords(
                SampleData.mergedRecord1112,
                CollapserUtils.merge(SampleData.metadataRecord11, SampleData.metadataRecord12));
        
        SampleData.assertEqualRecords(
                SampleData.mergedRecord2221,
                CollapserUtils.merge(SampleData.metadataRecord22, SampleData.metadataRecord21));
    }
    
    @Test
    public void testGetNestedFieldValue() {
        assertNull(CollapserUtils.getNestedFieldValue(null, null));
        assertNull(CollapserUtils.getNestedFieldValue(SampleData.metadataRecord11, null));
        assertNull(CollapserUtils.getNestedFieldValue(null, "notnull"));
        
        assertNull(CollapserUtils.getNestedFieldValue(SampleData.metadataRecord11, "field"));
        assertNull(CollapserUtils.getNestedFieldValue(SampleData.metadataRecord11, "id.field"));
        
        assertEquals("id-1", CollapserUtils.getNestedFieldValue(SampleData.metadataRecord11, "id"));
        assertEquals(1990+"", CollapserUtils.getNestedFieldValue(SampleData.metadataRecord11, "year"));
        assertEquals(true, CollapserUtils.getNestedFieldValue(SampleData.metadataRecord11, "publicationType.article"));
        
        assertEquals("id-1", CollapserUtils.getNestedFieldValue(SampleData.envMetadataRecord11, "data.id"));
        assertEquals(1990+"", CollapserUtils.getNestedFieldValue(SampleData.envMetadataRecord11, "data.year"));
        assertEquals(true, CollapserUtils.getNestedFieldValue(SampleData.envMetadataRecord11, "data.publicationType.article"));
    }
    
}
