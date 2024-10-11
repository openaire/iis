package pl.edu.icm.coansys.citations;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableList;

import pl.edu.icm.coansys.citations.data.HeuristicHashMatchingResult;
import pl.edu.icm.coansys.citations.data.IdWithSimilarity;
import pl.edu.icm.coansys.citations.data.MatchableEntity;
import scala.Tuple2;

/**
 * @author madryk
 */
public class CoreCitationMatchingServiceTest {

    private CoreCitationMatchingService coreCitationMatchingService = new CoreCitationMatchingService();
    
    @Mock
    private DocumentAttacher documentAttacher;
    @Mock
    private CitationAttacherWithMatchedLimiter citationAttacher;
    @Mock
    private BestMatchedCitationPicker bestMatchedCitationPicker;
    @Mock
    private HeuristicHashCitationMatcherFactory heuristicHashCitationMatcherFactory;
    
    @Mock
    private JavaSparkContext sparkContext;
    @Captor
    private ArgumentCaptor<List<Tuple2<String, String>>> listCaptor;
    
    
    @Mock
    private JavaPairRDD<String, MatchableEntity> citations;
    @Mock
    private JavaPairRDD<String, MatchableEntity> documents;
    
    @Mock
    private JavaPairRDD<String, String> matched1;
    @Mock
    private JavaPairRDD<String, MatchableEntity> unmatched1;
    @Mock
    private JavaPairRDD<String, String> matched2;
    
    @Mock
    private JavaPairRDD<String, String> emptyRDD;
    @Mock
    private JavaPairRDD<String, String> joinedMatched1;
    @Mock
    private JavaPairRDD<String, String> joinedMatched2;
    
    @Mock
    private JavaPairRDD<String, MatchableEntity> documentAttached;
    @Mock
    private JavaPairRDD<MatchableEntity, MatchableEntity> citationAttached;
    @Mock
    private JavaPairRDD<MatchableEntity, IdWithSimilarity> matchedCitations;
    
    @Mock
    private MatchableEntityHasher citationHasher1;
    @Mock
    private MatchableEntityHasher documentHasher1;
    @Mock
    private MatchableEntityHasher citationHasher2;
    @Mock
    private MatchableEntityHasher documentHasher2;
    
    
    @BeforeEach
    public void setup() {
        MockitoAnnotations.initMocks(this);
        
        coreCitationMatchingService.setDocumentAttacher(documentAttacher);
        coreCitationMatchingService.setCitationAttacher(citationAttacher);
        coreCitationMatchingService.setBestMatchedCitationPicker(bestMatchedCitationPicker);
        coreCitationMatchingService.setHeuristicHashCitationMatcherFactory(heuristicHashCitationMatcherFactory);
        
        List<Pair<MatchableEntityHasher, MatchableEntityHasher>> matchableEntitiesHashers = ImmutableList.of(
                new ImmutablePair<>(citationHasher1, documentHasher1),
                new ImmutablePair<>(citationHasher2, documentHasher2));
        
        coreCitationMatchingService.setSparkContext(sparkContext);
        coreCitationMatchingService.setMatchableEntityHashers(matchableEntitiesHashers);
        coreCitationMatchingService.setMaxHashBucketSize(100);
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void matchCitations() {
        
        // given
        
        HeuristicHashCitationMatcher heuristicHashCitationMatcher1 = mock(HeuristicHashCitationMatcher.class);
        HeuristicHashCitationMatcher heuristicHashCitationMatcher2 = mock(HeuristicHashCitationMatcher.class);
        
        when(heuristicHashCitationMatcherFactory.create(citationHasher1, documentHasher1, 100)).thenReturn(heuristicHashCitationMatcher1);
        when(heuristicHashCitationMatcherFactory.create(citationHasher2, documentHasher2, 100)).thenReturn(heuristicHashCitationMatcher2);
        
        when(heuristicHashCitationMatcher1.matchCitations(citations, documents, true)).thenReturn(new HeuristicHashMatchingResult(matched1, unmatched1));
        when(heuristicHashCitationMatcher2.matchCitations(unmatched1, documents, false)).thenReturn(new HeuristicHashMatchingResult(matched2, null));
        
        doReturn(emptyRDD).when(sparkContext).parallelizePairs(any());
        doReturn(joinedMatched1).when(emptyRDD).union(matched1);
        doReturn(joinedMatched2).when(joinedMatched1).union(matched2);
        
        when(documentAttacher.attachDocuments(joinedMatched2, documents)).thenReturn(documentAttached);
        when(citationAttacher.attachCitationsAndLimitDocs(documentAttached, citations)).thenReturn(citationAttached);
        when(bestMatchedCitationPicker.pickBest(citationAttached)).thenReturn(matchedCitations);
        
        
        // execute
        
        JavaPairRDD<MatchableEntity, IdWithSimilarity> retMatchedCitations = coreCitationMatchingService.matchCitations(citations, documents);
        
        
        // assert
        
        assertTrue(retMatchedCitations == matchedCitations);
        
        verify(heuristicHashCitationMatcherFactory).create(citationHasher1, documentHasher1, 100);
        verify(heuristicHashCitationMatcherFactory).create(citationHasher2, documentHasher2, 100);
        
        verify(heuristicHashCitationMatcher1).matchCitations(citations, documents, true);
        verify(heuristicHashCitationMatcher2).matchCitations(unmatched1, documents, false);
        
        verify(sparkContext).parallelizePairs(listCaptor.capture());
        assertTrue(listCaptor.getValue().isEmpty());
        verify(emptyRDD).union(matched1);
        verify(joinedMatched1).union(matched2);
        
        verify(documentAttacher).attachDocuments(joinedMatched2, documents);
        verify(citationAttacher).attachCitationsAndLimitDocs(documentAttached, citations);
        verify(bestMatchedCitationPicker).pickBest(citationAttached);
        
    }
    
}
