package pl.edu.icm.coansys.citations;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;

import pl.edu.icm.coansys.citations.data.MatchableEntity;
import pl.edu.icm.coansys.citations.hashers.HashGenerator;
import scala.Tuple2;

/**
 * Hasher of {@link MatchableEntity} objects
 * 
 * @author Łukasz Dumiszewski
 */

public class MatchableEntityHasher implements Serializable {

    private static final long serialVersionUID = 1L;
    	
    private HashGenerator hashGenerator;
    
    private ScalaIterableConverter scalaIterableConverter = new ScalaIterableConverter();
    
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Hashes the given {@link MatchableEntity} by using {@link #setHashGenerator(HashGenerator)}. Returns
     * list of tuples of the generated hashes and {@link MatchableEntity#id()} - (hash, entity.id()
     */
    public List<Tuple2<String, String>> hashEntity(MatchableEntity entity) {
        
        Preconditions.checkNotNull(entity);
        
        List<Tuple2<String, String>> hashIdPairs = new ArrayList<>();
        
        Iterable<String> hashes = scalaIterableConverter.convertToJavaIterable(hashGenerator.generate(entity));
        
        for (String hash : hashes) {
            hashIdPairs.add(new Tuple2<>(hash, entity.id()));
        }
        
        return hashIdPairs;
     }

    
    

    
    //------------------------ SETTERS --------------------------
    
    public void setHashGenerator(HashGenerator hashGenerator) {
        this.hashGenerator = hashGenerator;
    }
    
    void setScalaIterableConverter(ScalaIterableConverter scalaIterableConverter) {
        this.scalaIterableConverter = scalaIterableConverter;
    }


}
    
