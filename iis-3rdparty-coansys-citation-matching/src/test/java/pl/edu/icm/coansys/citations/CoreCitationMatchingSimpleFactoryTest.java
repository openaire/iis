package pl.edu.icm.coansys.citations;

import static org.mockito.Mockito.mock;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.JavaSparkContext;
import org.powermock.reflect.Whitebox;
import org.junit.jupiter.api.Test;

import pl.edu.icm.coansys.citations.hashers.CitationNameYearHashGenerator;
import pl.edu.icm.coansys.citations.hashers.CitationNameYearPagesHashGenerator;
import pl.edu.icm.coansys.citations.hashers.DocumentNameYearHashGenerator;
import pl.edu.icm.coansys.citations.hashers.DocumentNameYearNumNumHashGenerator;
import pl.edu.icm.coansys.citations.hashers.DocumentNameYearPagesHashGenerator;
import pl.edu.icm.coansys.citations.hashers.DocumentNameYearStrictHashGenerator;

/**
 * @author madryk
 */
public class CoreCitationMatchingSimpleFactoryTest {

    private CoreCitationMatchingSimpleFactory coreCitationMatchingFactory = new CoreCitationMatchingSimpleFactory();
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void createCoreCitationMatchingService() {
        
        // given
        
        JavaSparkContext sparkContext = mock(JavaSparkContext.class);
        
        
        // execute
        
        CoreCitationMatchingService citationMatchingService = coreCitationMatchingFactory.createCoreCitationMatchingService(sparkContext, 1000);
        
        
        // assert
        
        Object retSparkContext = Whitebox.getInternalState(citationMatchingService, "sparkContext");
        assertTrue(retSparkContext == sparkContext);
        
        Object retMaxHashBucketSize = Whitebox.getInternalState(citationMatchingService, "maxHashBucketSize");
        assertEquals(retMaxHashBucketSize, 1000l);
        
        
        @SuppressWarnings("unchecked")
        List<Pair<MatchableEntityHasher, MatchableEntityHasher>> retHashers = (List<Pair<MatchableEntityHasher, MatchableEntityHasher>>) Whitebox.getInternalState(citationMatchingService, "matchableEntityHashers");
        
        assertEquals(retHashers.size(), 4);
        
        assertHashersPair(retHashers.get(0), CitationNameYearPagesHashGenerator.class, DocumentNameYearPagesHashGenerator.class);
        assertHashersPair(retHashers.get(1), CitationNameYearPagesHashGenerator.class, DocumentNameYearNumNumHashGenerator.class);
        assertHashersPair(retHashers.get(2), CitationNameYearHashGenerator.class, DocumentNameYearStrictHashGenerator.class);
        assertHashersPair(retHashers.get(3), CitationNameYearHashGenerator.class, DocumentNameYearHashGenerator.class);
        
        
        Object retDocumentAttacher = Whitebox.getInternalState(citationMatchingService, "documentAttacher");
        assertNotNull(retDocumentAttacher);
        
        Object retCitationAttacher = Whitebox.getInternalState(citationMatchingService, "citationAttacher");
        assertNotNull(retCitationAttacher);
        
        Object retBestPicker = Whitebox.getInternalState(citationMatchingService, "bestMatchedCitationPicker");
        assertNotNull(retBestPicker);
        
        Object retHashCitationMatcherFactory = Whitebox.getInternalState(citationMatchingService, "heuristicHashCitationMatcherFactory");
        assertNotNull(retHashCitationMatcherFactory);
        
        
    }
    
    private void assertHashersPair(Pair<MatchableEntityHasher, MatchableEntityHasher> actualHashersPair, Class<?> expectedCitHashGeneratorClass, Class<?> expectedDocHashGeneratorClass) {
        
        Object citHashGenerator = Whitebox.getInternalState(actualHashersPair.getLeft(), "hashGenerator");
        assertTrue(expectedCitHashGeneratorClass.isInstance(citHashGenerator));
        
        Object docHashGenerator = Whitebox.getInternalState(actualHashersPair.getRight(), "hashGenerator");
        assertTrue(expectedDocHashGeneratorClass.isInstance(docHashGenerator));
        
    }
}
