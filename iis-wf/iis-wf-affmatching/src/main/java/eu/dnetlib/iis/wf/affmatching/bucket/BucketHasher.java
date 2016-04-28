package eu.dnetlib.iis.wf.affmatching.bucket;

import java.io.Serializable;

/**
 * Hasher of an object. Objects that will have the same hash can be considered to be in the same group (bucket).
 * 
 * @author Łukasz Dumiszewski
*/

public interface BucketHasher<T> extends Serializable {

    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns a hash of the given object. 
     */
    public String hash(T object);
    
}
