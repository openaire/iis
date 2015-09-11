package eu.dnetlib.iis.workflows.collapsers.origins;

import eu.dnetlib.iis.collapsers.schemas.DocumentTextEnvelope;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.workflows.collapsers.SampleData;
import eu.dnetlib.iis.workflows.collapsers.origins.DocumentTextCollapser;

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
public class DocumentTextCollapserTest {

    public static final List<String> origins = Lists.newArrayList("origin1", "origin2");
    
    public static final DocumentTextEnvelope record1 = DocumentTextEnvelope.newBuilder()
            .setOrigin("origin1")
            .setData(DocumentText.newBuilder().setId("id").setText("This is text").build())
            .build();
    
    public static final DocumentTextEnvelope record2 = DocumentTextEnvelope.newBuilder()
            .setOrigin("origin2")
            .setData(DocumentText.newBuilder().setId("id").setText("This is text").build())
            .build();
    
    public static final DocumentTextEnvelope record3 = DocumentTextEnvelope.newBuilder()
            .setOrigin("origin2")
            .setData(DocumentText.newBuilder().setId("id").setText("This is another text").build())
            .build();
    
    public static final DocumentTextEnvelope record4 = DocumentTextEnvelope.newBuilder()
            .setOrigin("origin2")
            .setData(DocumentText.newBuilder().setId("id").setText("This is a duplicated text").build())
            .build();

    
    public static final DocumentText collapsed1 = DocumentText.newBuilder()
            .setId("id")
            .setText("This is text").build();
    
    public static final DocumentText collapsed2 = DocumentText.newBuilder()
            .setId("id")
            .setText("This is text\n\nThis is text\n\nThis is another text\n\nThis is a duplicated text").build();
    
    
    public static final List<DocumentTextEnvelope> oneElementList = 
            Lists.newArrayList(record1);

    public static final List<DocumentText> collapsedOneElementList = 
            Lists.newArrayList(collapsed1);
    
    public static final List<DocumentTextEnvelope> list =
            Lists.newArrayList(record1, record2, record3, record4);
    
    public static final List<DocumentText> collapsedList =
            Lists.newArrayList(collapsed2);
    
    
    @Test
	public void testDocumentTextCollapserEmpty() throws Exception {
        DocumentTextCollapser collapser = new DocumentTextCollapser();
        collapser.setOrigins(origins);
        
        assertNull(collapser.collapse(null));
        assertNull(collapser.collapse(new ArrayList<DocumentTextEnvelope>()));
    }
    
    @Test
	public void testDocumentTextCollapser() throws Exception {
        DocumentTextCollapser collapser = new DocumentTextCollapser();
        collapser.setOrigins(origins);
        
        SampleData.assertEqualRecords(
                collapsedOneElementList,
                collapser.collapse(oneElementList));
        SampleData.assertEqualRecords(
                collapsedList,
                collapser.collapse(list));
    }

}
