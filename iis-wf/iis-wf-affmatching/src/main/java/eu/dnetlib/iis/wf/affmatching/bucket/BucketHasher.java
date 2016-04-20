package eu.dnetlib.iis.wf.affmatching.bucket;

import java.io.Serializable;

/**
* @author Łukasz Dumiszewski
*/

public interface BucketHasher<T> extends Serializable {

    
    public String hash(T object);
    
}
