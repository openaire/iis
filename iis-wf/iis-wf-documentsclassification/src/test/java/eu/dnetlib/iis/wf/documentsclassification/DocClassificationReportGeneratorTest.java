package eu.dnetlib.iis.wf.documentsclassification;

import com.google.common.collect.Lists;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.documentsclassification.schemas.DocumentClass;
import eu.dnetlib.iis.documentsclassification.schemas.DocumentClasses;
import eu.dnetlib.iis.documentsclassification.schemas.DocumentToDocumentClasses;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static eu.dnetlib.iis.common.report.ReportEntryFactory.createCounterReportEntry;
import static eu.dnetlib.iis.wf.documentsclassification.DocClassificationReportCounterKeys.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

/**
* @author Łukasz Dumiszewski
*/
@ExtendWith(MockitoExtension.class)
public class DocClassificationReportGeneratorTest {

    private DocClassificationReportGenerator reportGenerator = new DocClassificationReportGenerator();
    
    @Mock
    private JavaRDD<DocumentToDocumentClasses> documentClasses;
    
    @Mock
    private JavaRDD<Long> docClassCounts;
    
    @Mock
    private DocumentToDocumentClasses docClass;
    
    @Mock
    private DocumentClasses docClasses;
    

    @Captor
    private ArgumentCaptor<Function<DocumentToDocumentClasses, Long>> mapFunction;
    
    @Captor
    private ArgumentCaptor<Function2<Long, Long, Long>> reduceFunction;
    
    
    
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void generateReport_NULL() {
        
        // execute
        assertThrows(NullPointerException.class, () -> reportGenerator.generateReport(null));
        
    }
    
    
    
    @Test
    public void generateReport_empty() {
        
        // given
        
        when(documentClasses.count()).thenReturn(0L);
        
        // execute
        
        List<ReportEntry> reportEntries = reportGenerator.generateReport(documentClasses);
        
        // assert
        
        assertReportEntries(reportEntries, 0, 0, 0, 0, 0, 0);
        
        
    }

    
    @Test
    public void generateReport() throws Exception {
        
        // given
        
        when(documentClasses.count()).thenReturn(100L);
        
        doReturn(docClassCounts).when(documentClasses).map(Mockito.any());
        
        doReturn(2L).doReturn(3L).doReturn(4L).doReturn(5L).doReturn(6L).when(docClassCounts).reduce(Mockito.any());
        
        
        // execute
        
        List<ReportEntry> reportEntries = reportGenerator.generateReport(documentClasses);
        
        // assert
        
        assertReportEntries(reportEntries, 100, 2, 3, 4, 5, 6);
        
        verify(documentClasses, Mockito.times(5)).map(mapFunction.capture());
        assertArxivMapFunction(mapFunction.getAllValues().get(0));
        assertWosMapFunction(mapFunction.getAllValues().get(1));
        assertDdcMapFunction(mapFunction.getAllValues().get(2));
        assertMeshMapFunction(mapFunction.getAllValues().get(3));
        assertAcmMapFunction(mapFunction.getAllValues().get(4));
        
        verify(docClassCounts, Mockito.times(5)).reduce(reduceFunction.capture());
        for (Function2<Long, Long, Long> rf : reduceFunction.getAllValues()) {
            assertReduceFunction(rf);
        }
    }

    
    //------------------------ PRIVATE --------------------------
    
    private void assertReportEntries(List<ReportEntry> reportEntries, long classifiedDocumentCount, long arxivClassCount, long wosClassCount, long ddcClassCount, long meshEuroPmcClassCount, long acmClassCount) {
        
        assertThat(reportEntries, Matchers.contains(createCounterReportEntry(CLASSIFIED_DOCUMENTS, classifiedDocumentCount),
                                                    createCounterReportEntry(ARXIV_CLASSES, arxivClassCount),
                                                    createCounterReportEntry(WOS_CLASSES, wosClassCount),
                                                    createCounterReportEntry(DDC_CLASSES, ddcClassCount),
                                                    createCounterReportEntry(MESH_EURO_PMC_CLASSES, meshEuroPmcClassCount),
                                                    createCounterReportEntry(ACM_CLASSES, acmClassCount)));
    }
    
    private void assertArxivMapFunction(Function<DocumentToDocumentClasses, Long> function) throws Exception {
        
        // given
        when(docClass.getClasses()).thenReturn(docClasses);
        when(docClasses.getArXivClasses()).thenReturn(generateDocumentClasses(2));
        
        // execute & assert
        assertEquals(2L, function.call(docClass).longValue());
    }

    private void assertWosMapFunction(Function<DocumentToDocumentClasses, Long> function) throws Exception {
        
        // given
        when(docClass.getClasses()).thenReturn(docClasses);
        when(docClasses.getWoSClasses()).thenReturn(generateDocumentClasses(4));
        
        // execute & assert
        assertEquals(4L, function.call(docClass).longValue());
    }

    private void assertDdcMapFunction(Function<DocumentToDocumentClasses, Long> function) throws Exception {
        
        // given
        when(docClass.getClasses()).thenReturn(docClasses);
        when(docClasses.getDDCClasses()).thenReturn(generateDocumentClasses(6));
        
        // execute & assert
        assertEquals(6L, function.call(docClass).longValue());
    }

    private void assertMeshMapFunction(Function<DocumentToDocumentClasses, Long> function) throws Exception {
        
        // given
        when(docClass.getClasses()).thenReturn(docClasses);
        when(docClasses.getMeshEuroPMCClasses()).thenReturn(generateDocumentClasses(17));
        
        // execute & assert
        assertEquals(17L, function.call(docClass).longValue());
    }

    private void assertAcmMapFunction(Function<DocumentToDocumentClasses, Long> function) throws Exception {
        
        // given
        when(docClass.getClasses()).thenReturn(docClasses);
        when(docClasses.getACMClasses()).thenReturn(generateDocumentClasses(7));
        
        // execute & assert
        assertEquals(7L, function.call(docClass).longValue());
    }
    
    private List<DocumentClass> generateDocumentClasses(int numberOfItems) {
        
        List<DocumentClass> docClasses = Lists.newArrayList();
        
        for (int i = 0; i < numberOfItems; i++) {
            docClasses.add(mock(DocumentClass.class));
        }
        
        return docClasses;
    }
    
    private void assertReduceFunction(Function2<Long, Long, Long> function) throws Exception {
        
        // execute & assert
        assertEquals(4L, function.call(1L, 3L).longValue());
        assertEquals(7L, function.call(4L, 3L).longValue());
    }
    
}
