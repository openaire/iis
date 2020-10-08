package eu.dnetlib.iis.common.model.extrainfo.citations;

import eu.dnetlib.iis.common.model.extrainfo.ExtraInfoConstants;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

public class AlphaNumericCitationComparatorTest {

	@Test
	public void testSorting() {
		SortedSet<BlobCitationEntry> citations = new TreeSet<BlobCitationEntry>();
		
		BlobCitationEntry c1 = new BlobCitationEntry("[1] test");
		BlobCitationEntry c2 = new BlobCitationEntry("[10] test");
		BlobCitationEntry c3 = new BlobCitationEntry("[2] test");
		BlobCitationEntry c4 = new BlobCitationEntry("[1] test");
		c4.setIdentifiers(new ArrayList<TypedId>());
		c4.getIdentifiers().add(new TypedId("1", ExtraInfoConstants.CITATION_TYPE_OPENAIRE, 0.9f));
		citations.add(c4);
		citations.add(c3);
		citations.add(c2);
		citations.add(c1);
		Iterator<BlobCitationEntry> citationsIt = citations.iterator();
		assertSame(c1, citationsIt.next());
		assertSame(c4, citationsIt.next());
		assertSame(c3, citationsIt.next());
		assertSame(c2, citationsIt.next());
		assertFalse(citationsIt.hasNext());
	}
	
	@Test
	public void testSortingWithNulls() {
		SortedSet<BlobCitationEntry> citations = new TreeSet<BlobCitationEntry>();
		BlobCitationEntry c2 = new BlobCitationEntry("[10] test");
		BlobCitationEntry c3 = new BlobCitationEntry(null);
		citations.add(c3);
		citations.add(c2);
		Iterator<BlobCitationEntry> citationsIt = citations.iterator();
		assertSame(c2, citationsIt.next());
		assertSame(c3, citationsIt.next());
	}
	
	@Test
	public void testSortingWithDuplicates() {
		SortedSet<BlobCitationEntry> citations = new TreeSet<BlobCitationEntry>();
		
		BlobCitationEntry c1 = new BlobCitationEntry("[1] test");
		BlobCitationEntry c2 = new BlobCitationEntry("[1] test");
		BlobCitationEntry c3 = new BlobCitationEntry("[2] test");
		BlobCitationEntry c4 = new BlobCitationEntry("[2] test");
		BlobCitationEntry c5 = new BlobCitationEntry("[2] test");
		c3.setIdentifiers(new ArrayList<TypedId>());
		c3.getIdentifiers().add(new TypedId("1", ExtraInfoConstants.CITATION_TYPE_OPENAIRE, 0.9f));
		c4.setIdentifiers(new ArrayList<TypedId>());
		c4.getIdentifiers().add(new TypedId("1", ExtraInfoConstants.CITATION_TYPE_OPENAIRE, 0.9f));
		c5.setIdentifiers(new ArrayList<TypedId>());
		c5.getIdentifiers().add(new TypedId("2", ExtraInfoConstants.CITATION_TYPE_OPENAIRE, 0.9f));
		citations.add(c5);
		citations.add(c4);
		citations.add(c3);
		citations.add(c2);
		citations.add(c1);
		Iterator<BlobCitationEntry> citationsIt = citations.iterator();
		assertSame(c2, citationsIt.next());
		assertSame(c5, citationsIt.next());
		assertSame(c4, citationsIt.next());
		assertFalse(citationsIt.hasNext());
	}
}
