package eu.dnetlib.iis.wf.collapsers.basic;

import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.collapsers.schemas.DocumentMetadata;
import eu.dnetlib.iis.wf.collapsers.SampleData;

/**
 * 
 * @author Dominika Tkaczyk
 *
 */
public class BestFilledCollapserTest {

    public static final List<DocumentMetadata> emptyList = 
            new ArrayList<DocumentMetadata>();
    
    public static final List<DocumentMetadata> list1 = 
            Lists.newArrayList(SampleData.metadataRecord11);
    
    public static final List<DocumentMetadata> list2 = 
            Lists.newArrayList(SampleData.metadataRecord12);

    public static final List<DocumentMetadata> list123 =
            Lists.newArrayList(SampleData.metadataRecord11, SampleData.metadataRecord12, SampleData.metadataRecord13);

    public static final List<DocumentMetadata> list321 =
            Lists.newArrayList(SampleData.metadataRecord13, SampleData.metadataRecord12, SampleData.metadataRecord11);
    
    
    @Test
	public void testBestFilledEmpty() throws Exception {
        BestFilledCollapser<DocumentMetadata> collapser = new BestFilledCollapser<DocumentMetadata>();
        
        assertNull(collapser.collapse(null));
        assertNull(collapser.collapse(emptyList));
    }
    
    @Test
	public void testBestFilledDefaultFieldSet() throws Exception {
        BestFilledCollapser<DocumentMetadata> collapser = new BestFilledCollapser<DocumentMetadata>();
        
        SampleData.assertEqualRecords(
                list1,
                collapser.collapse(list1));
        SampleData.assertEqualRecords(
                list1,
                collapser.collapse(list123));
        SampleData.assertEqualRecords(
                list1, 
                collapser.collapse(list321));
    }

    @Test
	public void testBestFilled() throws Exception {
        BestFilledCollapser<DocumentMetadata> collapser = new BestFilledCollapser<DocumentMetadata>();
        collapser.setFields(SampleData.significantFields);
        
        SampleData.assertEqualRecords(
                list1,
                collapser.collapse(list1));
        SampleData.assertEqualRecords(
                list2, 
                collapser.collapse(list123));
        SampleData.assertEqualRecords(
                list2, 
                collapser.collapse(list321));
    }

}
