package eu.dnetlib.iis.wf.collapsers.basic;

import com.google.common.collect.Lists;
import eu.dnetlib.iis.collapsers.schemas.DocumentMetadata;
import eu.dnetlib.iis.wf.collapsers.SampleData;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 
 * @author Dominika Tkaczyk
 *
 */
public class BestFilledMergingCollapserTest {

    public static final List<DocumentMetadata> emptyList = 
            new ArrayList<DocumentMetadata>();
    
    public static final List<DocumentMetadata> list1 = 
            Lists.newArrayList(SampleData.metadataRecord11);
    
    public static final List<DocumentMetadata> mergedList12 = 
            Lists.newArrayList(SampleData.mergedRecord1112);

    public static final List<DocumentMetadata> mergedList21 = 
            Lists.newArrayList(SampleData.mergedRecord1211);
    
    public static final List<DocumentMetadata> list123 =
            Lists.newArrayList(SampleData.metadataRecord11, SampleData.metadataRecord12, SampleData.metadataRecord13);

    public static final List<DocumentMetadata> list321 =
            Lists.newArrayList(SampleData.metadataRecord13, SampleData.metadataRecord12, SampleData.metadataRecord11);
    
    
    @Test
	public void testBestFilledEmpty() throws Exception {
        BestFilledMergingCollapser<DocumentMetadata> collapser = new BestFilledMergingCollapser<DocumentMetadata>();
        
        assertNull(collapser.collapse(null));
        assertNull(collapser.collapse(emptyList));
    }
    
    @Test
	public void testBestFilledMergingDefaultFieldSet() throws Exception {
        BestFilledMergingCollapser<DocumentMetadata> collapser = new BestFilledMergingCollapser<DocumentMetadata>();
                
        SampleData.assertEqualRecords(
                list1,
                collapser.collapse(list1));
        SampleData.assertEqualRecords(
                mergedList12,
                collapser.collapse(list321));
    }
    
    @Test
	public void testBestFilledMerging() throws Exception {
        BestFilledMergingCollapser<DocumentMetadata> collapser = new BestFilledMergingCollapser<DocumentMetadata>();
        collapser.setFields(SampleData.significantFields);
        
        SampleData.assertEqualRecords(
                list1,
                collapser.collapse(list1));
        SampleData.assertEqualRecords(
                mergedList21,
                collapser.collapse(list321));
    }
   
}
