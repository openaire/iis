package eu.dnetlib.iis.workflows.collapsers.origins;

import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.collapsers.schemas.DocumentMetadata;
import eu.dnetlib.iis.collapsers.schemas.DocumentMetadataEnvelope;
import eu.dnetlib.iis.workflows.collapsers.SampleData;

/**
 * 
 * @author Dominika Tkaczyk
 *
 */
public class OriginConfidenceMergingCollapserTest {

    public static final List<DocumentMetadataEnvelope> emptyList = new ArrayList<DocumentMetadataEnvelope>();
  
    public static final List<DocumentMetadataEnvelope> oneElementList = 
            Lists.newArrayList(SampleData.envMetadataRecord11);
    
    public static final List<DocumentMetadata> mergedOneElementList = 
            Lists.newArrayList(SampleData.metadataRecord11);
    
    public static final List<DocumentMetadataEnvelope> list = 
            Lists.newArrayList(SampleData.envMetadataRecord11, SampleData.envMetadataRecord21);
    
    public static final List<DocumentMetadata> mergedList = 
            Lists.newArrayList(SampleData.mergedRecord1121);
    
    
    @Test
	public void testOriginConfidenceMerging() throws Exception {
        OriginConfidenceMergingCollapser<DocumentMetadataEnvelope, DocumentMetadata> collapser = 
                new OriginConfidenceMergingCollapser<DocumentMetadataEnvelope, DocumentMetadata>();
        collapser.setOrigins(SampleData.origins);

        assertNull(collapser.collapse(null));
        assertNull(collapser.collapse(emptyList));
        SampleData.assertEqualRecords(
                mergedOneElementList,
                collapser.collapse(oneElementList));
        SampleData.assertEqualRecords(
                mergedList,
                collapser.collapse(list));
    }
    
}
