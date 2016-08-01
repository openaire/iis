package eu.dnetlib.iis.wf.affmatching.match.voter;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;

/**
* @author Łukasz Dumiszewski
*/
@RunWith(MockitoJUnitRunner.class)
public class CommonSimilarWordCalculatorTest {

    private double MIN_WORD_SIMILARITY = 0.83;
    
    @InjectMocks
    private CommonSimilarWordCalculator calculator = new CommonSimilarWordCalculator(MIN_WORD_SIMILARITY);
    
    @Mock
    private StringSimilarityChecker similarityChecker;
    
    @Mock
    private List<String> inWords;
    
    
    
    //------------------------ TESTS --------------------------
    
    
    @Test(expected = NullPointerException.class)
    public void calcSimilarWordNumber_FindWords_NULL() {
        
        // execute
        
        calculator.calcSimilarWordNumber(null, ImmutableList.of("A"));
        
    }

    
    @Test(expected = NullPointerException.class)
    public void calcSimilarWordNumber_InWords_NULL() {
        
        // execute
        
        calculator.calcSimilarWordNumber(ImmutableList.of("A"), null);
        
    }
    
    
    @Test
    public void calcSimilarWordNumber() {
        
        // given
        
        List<String> findWords = ImmutableList.of("electronics", "department", "university", "warsaw");
        
        when(similarityChecker.containsSimilarString(inWords, "electronics", MIN_WORD_SIMILARITY)).thenReturn(false);
        when(similarityChecker.containsSimilarString(inWords, "departments", MIN_WORD_SIMILARITY)).thenReturn(false);
        when(similarityChecker.containsSimilarString(inWords, "university", MIN_WORD_SIMILARITY)).thenReturn(true);
        when(similarityChecker.containsSimilarString(inWords, "warsaw", MIN_WORD_SIMILARITY)).thenReturn(true);
        
        
        // execute
        
        int numberOfSimilarWords = calculator.calcSimilarWordNumber(findWords, inWords);
        
        
        // assert
        
        assertEquals(2, numberOfSimilarWords);
        
    }
    
    
    
    @Test
    public void calcSimilarWordRatio() {
        
        // given
        
        List<String> findWords = ImmutableList.of("electronics", "department", "university", "warsaw");
        
        when(similarityChecker.containsSimilarString(inWords, "electronics", MIN_WORD_SIMILARITY)).thenReturn(false);
        when(similarityChecker.containsSimilarString(inWords, "departments", MIN_WORD_SIMILARITY)).thenReturn(false);
        when(similarityChecker.containsSimilarString(inWords, "university", MIN_WORD_SIMILARITY)).thenReturn(true);
        when(similarityChecker.containsSimilarString(inWords, "warsaw", MIN_WORD_SIMILARITY)).thenReturn(true);
        
        
        // execute
        
        double similarWordRatio = calculator.calcSimilarWordRatio(findWords, inWords);
        
        
        // assert
        
        assertEquals(0.5, similarWordRatio, 0.000001);
        
    }
}
