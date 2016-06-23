package eu.dnetlib.iis.wf.affmatching.normalize;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Sets;

import eu.dnetlib.iis.common.string.StringNormalizer;

/**
 * @author madryk
 */
@RunWith(MockitoJUnitRunner.class)
public class OrganizationNameNormalizerTest {

    @InjectMocks
    private OrganizationNameNormalizer normalizer = new OrganizationNameNormalizer();
    
    @Mock
    private StringNormalizer innerNormalizer;
    
    private Set<String> stopwords = Sets.newHashSet("stop", "stop2");
    
    
    @Before
    public void setup() {
        normalizer.setStopwords(stopwords);
        when(innerNormalizer.normalize(any())).thenAnswer(x -> x.getArgumentAt(0, String.class));
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void normalize() {

        // execute
        String filtered = normalizer.normalize("some text");
        
        // assert
        assertEquals("some text", filtered);
        verify(innerNormalizer).normalize("some text");
        
    }
    
    @Test
    public void normalize_NULL() {
        // execute & assert
        assertEquals("", normalizer.normalize(null));
    }
    
    @Test
    public void normalize_BLANK() {
        // execute & assert
        assertEquals("", normalizer.normalize("   "));
    }
    
    @Test
    public void normalize_BRACKETS() {
        
        // execute
        String filtered = normalizer.normalize("some text (with brackets)");
        
        // assert
        assertEquals("some text", filtered);
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
    
    @Test
    public void normalize_STOPWORDS() {

        // execute
        String filtered = normalizer.normalize("first stop second stop2 stop this words stop");
        
        // assert
        assertEquals("first second this words", filtered);
        verify(innerNormalizer).normalize("first stop second stop2 stop this words stop");
        
    }
}
