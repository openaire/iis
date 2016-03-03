package eu.dnetlib.iis.common.java.io;

import java.util.Iterator;

/**
 * Counting iterator providing total number of results.
 * @author mhorst
 *
 * @param <E>
 */
public interface CountingIterator<E> extends Iterator<E> {

	/**
	 * Provides total number of results to be iterating on.
	 * @return total number of results to be iterating on
	 */
	int getCount();
	
}
