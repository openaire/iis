package eu.dnetlib.iis.wf.affmatching.normalize;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.common.string.StringNormalizer;

/**
 * @author madryk
 */
@RunWith(MockitoJUnitRunner.class)
public class BracketsPreFilteringNormalizerTest {

    private BracketsPreFilteringNormalizer normalizer;
    
    @Mock
    private StringNormalizer innerNormalizer;
    
    @Before
    public void setup() {
        when(innerNormalizer.normalize(any())).thenAnswer(x -> x.getArgumentAt(0, String.class));
        
        normalizer = new BracketsPreFilteringNormalizer(innerNormalizer);
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void normalize() {
        
        // execute
        String filtered = normalizer.normalize("some text (with brackets)");
        
        // assert
        assertEquals("some text ", filtered);
        verify(innerNormalizer).normalize("some text ");
        
    }
    
    @Test
    public void normalize_MANY_BRACKET_PAIRS() {
        
        // execute
        String filtered = normalizer.normalize("(aa)some (bb)more complex(cc) text");
        
        // assert
        assertEquals("some more complex text", filtered);
        verify(innerNormalizer).normalize("some more complex text");
        
    }
}
