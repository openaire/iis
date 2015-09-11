package eu.dnetlib.iis.workflows.collapsers.origins;

import eu.dnetlib.iis.collapsers.schemas.DocumentMetadataEnvelope;
import eu.dnetlib.iis.importer.schemas.DocumentMetadata;
import eu.dnetlib.iis.workflows.collapsers.SampleData;
import eu.dnetlib.iis.workflows.collapsers.origins.OriginConfidenceCollapser;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNull;

import org.junit.Test;
import org.python.google.common.collect.Lists;

/**
 * 
 * @author Dominika Tkaczyk
 *
 */
public class OriginConfidenceCollapserTest {
    
    public static final List<DocumentMetadataEnvelope> emptyList = new ArrayList<DocumentMetadataEnvelope>();
  
    public static final List<DocumentMetadataEnvelope> oneElementList = 
            Lists.newArrayList(SampleData.envMetadataRecord11);
    
    public static final List<DocumentMetadata> mergedOneElementList = 
            Lists.newArrayList(SampleData.metadataRecord11);
    
    public static final List<DocumentMetadataEnvelope> list = 
            Lists.newArrayList(SampleData.envMetadataRecord11, SampleData.envMetadataRecord21);
    
    
    @Test
	public void testOriginConfidence() throws Exception {
        OriginConfidenceCollapser<DocumentMetadataEnvelope, DocumentMetadata> collapser = 
                new OriginConfidenceCollapser<DocumentMetadataEnvelope, DocumentMetadata>();
        collapser.setOrigins(SampleData.origins);
        
        assertNull(collapser.collapse(null));
        assertNull(collapser.collapse(emptyList));
        SampleData.assertEqualRecords(
                mergedOneElementList,
                collapser.collapse(oneElementList));
        SampleData.assertEqualRecords(
                mergedOneElementList,
                collapser.collapse(list));
    }
    
}
