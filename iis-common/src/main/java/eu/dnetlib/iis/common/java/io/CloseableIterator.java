package eu.dnetlib.iis.common.java.io;

import java.io.Closeable;
import java.util.Iterator;

/**
 * An iterator for I/O operations that can be {@code close}d explicitly to 
 * release the resources it holds. 
 * 
 * You should call {@code close} only when interrupting the iteration in the 
 * middle since in such situation there is no way for the iterator to know if 
 * you're going to continue the iteration and it should still hold the resources 
 * or not. There's no need to call {@code close} when iterating over all 
 * elements since in such situation it is called automatically after the 
 * end of iteration.
 * 
 * @author mhorst
 *
 * @param <E>
 */
public interface CloseableIterator<E> extends Iterator<E>, Closeable {

	
}

