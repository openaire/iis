package pl.edu.icm.coansys.citations;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.mockito.Mockito;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

import pl.edu.icm.coansys.citations.MatchableEntityHasher;
import pl.edu.icm.coansys.citations.ScalaIterableConverter;
import pl.edu.icm.coansys.citations.data.MatchableEntity;
import pl.edu.icm.coansys.citations.hashers.HashGenerator;
import scala.Tuple2;

/**
* @author Łukasz Dumiszewski
*/
public class MatchableEntityHasherTest {


    private static MatchableEntityHasher matchableEntityHasher = new MatchableEntityHasher();

    private static HashGenerator hashGenerator = mock(HashGenerator.class);
    
    private static ScalaIterableConverter scalaIterableConverter = mock(ScalaIterableConverter.class);
    
    
    
    @BeforeAll
    public static void beforeClass() {
        
        matchableEntityHasher.setHashGenerator(hashGenerator);
        matchableEntityHasher.setScalaIterableConverter(scalaIterableConverter);
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void hashEntity_NULL() {
        
        // execute
        
        assertThrows(NullPointerException.class, () -> matchableEntityHasher.hashEntity(null));
        
    }
    
    
    @Test
    public void hashEntity() {
        
        // given
        
        MatchableEntity entity = mock(MatchableEntity.class);
        when(entity.id()).thenReturn("12");
        
        @SuppressWarnings("unchecked")
        scala.collection.Iterable<String> iterable = mock(scala.collection.Iterable.class);
        
        Mockito.when(hashGenerator.generate(entity)).thenReturn(iterable);
        
        List<String> hashes = Lists.newArrayList("XXX", "ZZZ");
        when(scalaIterableConverter.convertToJavaIterable(iterable)).thenReturn(hashes);
        
        // execute
        
        List<Tuple2<String, String>> hashIdPairs = matchableEntityHasher.hashEntity(entity);
        
        // assert
        
        assertEquals(2, hashIdPairs.size());
        assertEquals(new Tuple2<>("XXX", "12"), hashIdPairs.get(0));
        assertEquals(new Tuple2<>("ZZZ", "12"), hashIdPairs.get(1));
        
    }
    
    
}
