package eu.dnetlib.iis.wf.affmatching.match.voter;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

/**
* @author Łukasz Dumiszewski
*/
@ExtendWith(MockitoExtension.class)
public class CommonSimilarWordCalculatorTest {

    private double MIN_WORD_SIMILARITY = 0.83;
    
    @InjectMocks
    private CommonSimilarWordCalculator calculator = new CommonSimilarWordCalculator(MIN_WORD_SIMILARITY);
    
    @Mock
    private StringSimilarityChecker similarityChecker;
    
    @Mock
    private List<String> inWords;
    
    
    
    //------------------------ TESTS --------------------------
    
    
    @Test
    public void calcSimilarWordNumber_FindWords_NULL() {
        
        // execute
        assertThrows(NullPointerException.class, () -> calculator.calcSimilarWordNumber(null, ImmutableList.of("A")));
        
    }

    
    @Test
    public void calcSimilarWordNumber_InWords_NULL() {
        
        // execute
        assertThrows(NullPointerException.class, () -> calculator.calcSimilarWordNumber(ImmutableList.of("A"), null));
        
    }
    
    
    @Test
    public void calcSimilarWordNumber() {
        
        // given
        
        List<String> findWords = ImmutableList.of("electronics", "department", "university", "warsaw");
        
        when(similarityChecker.containsSimilarString(inWords, "electronics", MIN_WORD_SIMILARITY)).thenReturn(false);
        when(similarityChecker.containsSimilarString(inWords, "department", MIN_WORD_SIMILARITY)).thenReturn(false);
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
        when(similarityChecker.containsSimilarString(inWords, "department", MIN_WORD_SIMILARITY)).thenReturn(false);
        when(similarityChecker.containsSimilarString(inWords, "university", MIN_WORD_SIMILARITY)).thenReturn(true);
        when(similarityChecker.containsSimilarString(inWords, "warsaw", MIN_WORD_SIMILARITY)).thenReturn(true);
        
        
        // execute
        
        double similarWordRatio = calculator.calcSimilarWordRatio(findWords, inWords);
        
        
        // assert
        
        assertEquals(0.5, similarWordRatio, 0.000001);
        
    }
}
