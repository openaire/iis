package eu.dnetlib.iis.common.model.extrainfo.citations;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Test;

public class AlphaNumericCitationComparatorTest {

	@Test
	public void testSorting() throws Exception {
		SortedSet<BlobCitationEntry> citations = new TreeSet<BlobCitationEntry>();
		
		BlobCitationEntry c1 = new BlobCitationEntry("[1] test");
		BlobCitationEntry c2 = new BlobCitationEntry("[10] test");
		BlobCitationEntry c3 = new BlobCitationEntry("[2] test");
		BlobCitationEntry c4 = new BlobCitationEntry("[1] test");
		c4.setIdentifiers(new ArrayList<TypedId>());
		c4.getIdentifiers().add(new TypedId("1", "openaire", 0.9f));
		citations.add(c4);
		citations.add(c3);
		citations.add(c2);
		citations.add(c1);
		Iterator<BlobCitationEntry> citationsIt = citations.iterator();
		assertTrue(c1==citationsIt.next());
		assertTrue(c4==citationsIt.next());
		assertTrue(c3==citationsIt.next());
		assertTrue(c2==citationsIt.next());
		assertFalse(citationsIt.hasNext());
	}
	
	@Test
	public void testSortingWithNulls() throws Exception {
		SortedSet<BlobCitationEntry> citations = new TreeSet<BlobCitationEntry>();
		BlobCitationEntry c2 = new BlobCitationEntry("[10] test");
		BlobCitationEntry c3 = new BlobCitationEntry(null);
		citations.add(c3);
		citations.add(c2);
		Iterator<BlobCitationEntry> citationsIt = citations.iterator();
		assertTrue(c2==citationsIt.next());
		assertTrue(c3==citationsIt.next());
	}
	
	@Test
	public void testSortingWithDuplicates() throws Exception {
		SortedSet<BlobCitationEntry> citations = new TreeSet<BlobCitationEntry>();
		
		BlobCitationEntry c1 = new BlobCitationEntry("[1] test");
		BlobCitationEntry c2 = new BlobCitationEntry("[1] test");
		BlobCitationEntry c3 = new BlobCitationEntry("[2] test");
		BlobCitationEntry c4 = new BlobCitationEntry("[2] test");
		BlobCitationEntry c5 = new BlobCitationEntry("[2] test");
		c3.setIdentifiers(new ArrayList<TypedId>());
		c3.getIdentifiers().add(new TypedId("1", "openaire", 0.9f));
		c4.setIdentifiers(new ArrayList<TypedId>());
		c4.getIdentifiers().add(new TypedId("1", "openaire", 0.9f));
		c5.setIdentifiers(new ArrayList<TypedId>());
		c5.getIdentifiers().add(new TypedId("2", "openaire", 0.9f));
		citations.add(c5);
		citations.add(c4);
		citations.add(c3);
		citations.add(c2);
		citations.add(c1);
		Iterator<BlobCitationEntry> citationsIt = citations.iterator();
		assertTrue(c2==citationsIt.next());
		assertTrue(c5==citationsIt.next());
		assertTrue(c4==citationsIt.next());
		assertFalse(citationsIt.hasNext());
	}
}
