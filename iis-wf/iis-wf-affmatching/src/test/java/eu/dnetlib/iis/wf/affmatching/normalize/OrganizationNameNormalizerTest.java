package eu.dnetlib.iis.wf.affmatching.normalize;

import com.google.common.collect.Sets;
import eu.dnetlib.iis.common.string.StringNormalizer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

/**
 * @author madryk
 */
@ExtendWith(MockitoExtension.class)
public class OrganizationNameNormalizerTest {

    @InjectMocks
    private OrganizationNameNormalizer normalizer = new OrganizationNameNormalizer();
    
    @Mock
    private StringNormalizer innerNormalizer;
    
    private Set<String> stopwords = Sets.newHashSet("stop", "stop2");
    
    
    @BeforeEach
    public void setup() {
        normalizer.setStopwords(stopwords);
        lenient().when(innerNormalizer.normalize(any())).thenAnswer(invocation -> invocation.getArgument(0));
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
