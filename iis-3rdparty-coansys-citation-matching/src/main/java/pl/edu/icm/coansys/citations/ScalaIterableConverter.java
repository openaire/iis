package pl.edu.icm.coansys.citations;

import java.io.Serializable;

import scala.jdk.javaapi.CollectionConverters;

/**
 * Converter of {@link scala.collection.Iterable} to {@link java.lang.Iterable}
 *  
 * @author Łukasz Dumiszewski
*/

class ScalaIterableConverter implements Serializable {

    
    private static final long serialVersionUID = 1L;

    /**
     * Converts {@link scala.collection.Iterable} to {@link java.lang.Iterable}. The method is not static (as {@link CollectionConverters#asJava(scala.collection.Iterable)})
     * so it can be used as a service method and it is easier to test.
     */
    <T> Iterable<T> convertToJavaIterable(scala.collection.Iterable<T> scalaIterable) {
        return CollectionConverters.asJava(scalaIterable);
    }
    
}
