package eu.dnetlib.iis.workflows.collapsers.basic;

import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.workflows.collapsers.SampleData;
import eu.dnetlib.iis.workflows.collapsers.basic.DocumentTextCollapser;

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

    public static final List<DocumentText> oneElementList = 
            Lists.newArrayList(
                DocumentText.newBuilder().setId("id").setText("This is text").build()
            );
    
    public static final List<DocumentText> list =
            Lists.newArrayList(
                DocumentText.newBuilder().setId("id").setText("This is text").build(),
                DocumentText.newBuilder().setId("id").setText("This is another text").build(),
                DocumentText.newBuilder().setId("id").setText("This is a duplicated text").build()
            );

    public static final List<DocumentText> collapsedList =
            Lists.newArrayList(
                DocumentText.newBuilder().setId("id").setText("This is text\n\nThis is another text\n\nThis is a duplicated text").build()
            );
  
    
    @Test
	public void testDocumentTextCollapserEmpty() throws Exception {
        DocumentTextCollapser collapser = new DocumentTextCollapser();
   
        assertNull(collapser.collapse(null));
        assertNull(collapser.collapse(new ArrayList<DocumentText>()));
    }
    
    @Test
	public void testDocumentTextCollapser() throws Exception {
        DocumentTextCollapser collapser = new DocumentTextCollapser();
        
        SampleData.assertEqualRecords(
                oneElementList,
                collapser.collapse(oneElementList));
        SampleData.assertEqualRecords(
                collapsedList,
                collapser.collapse(list));
    }

}
