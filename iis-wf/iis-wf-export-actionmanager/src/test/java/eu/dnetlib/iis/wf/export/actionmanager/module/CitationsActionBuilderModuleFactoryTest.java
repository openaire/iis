package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Test;

import eu.dnetlib.iis.common.citations.schemas.CitationEntry;
import eu.dnetlib.iis.common.hbase.HBaseConstants;
import eu.dnetlib.iis.common.model.extrainfo.citations.BlobCitationEntry;
import eu.dnetlib.iis.common.model.extrainfo.converter.CitationsExtraInfoConverter;

/**
 * {@link CitationsExtraInfoConverter} test class.
 * @author mhorst
 *
 */
public class CitationsActionBuilderModuleFactoryTest {

	@Test
	public void testConversion() throws Exception {
		SortedSet<BlobCitationEntry> sortedCitations = new TreeSet<BlobCitationEntry>();
		
		CitationEntry.Builder rawTextCitationEntryBuilder = CitationEntry.newBuilder();
		rawTextCitationEntryBuilder.setPosition(44);
		rawTextCitationEntryBuilder.setRawText("[44] S. Mukhi and R. Nigam, “Constraints on ’rare’ dyon decays,” JHEP 12 (2008) 056, 0809.1157.");
		rawTextCitationEntryBuilder.setExternalDestinationDocumentIds(Collections.<CharSequence,CharSequence>emptyMap());
		
		CitationEntry.Builder internalCitationEntryBuilder = CitationEntry.newBuilder();
		internalCitationEntryBuilder.setRawText("Rugama, Y., Kloosterman, J. L., Winkelman, A., 2004. Prog. Nucl. Energy 44, 1-12.");
		internalCitationEntryBuilder.setPosition(100);
		internalCitationEntryBuilder.setDestinationDocumentId("od______2367::00247be440c2188b82d5905b5b1e22bb");
		internalCitationEntryBuilder.setConfidenceLevel(0.8f);
		internalCitationEntryBuilder.setExternalDestinationDocumentIds(Collections.<CharSequence,CharSequence>emptyMap());
		
		CitationEntry.Builder externalPmidCitationEntryBuilder = CitationEntry.newBuilder();
		externalPmidCitationEntryBuilder.setRawText("[5] A. Sen, “Walls of Marginal Stability and Dyon Spectrum in N=4 Supersymmetric String Theories,” JHEP 05 (2007) 039, hep-th/0702141.");
		externalPmidCitationEntryBuilder.setPosition(5);
		Map<CharSequence, CharSequence> externalPmidDestinationDocumentIds = new HashMap<CharSequence, CharSequence>();
		externalPmidDestinationDocumentIds.put("pmid", "20856923");
		externalPmidCitationEntryBuilder.setExternalDestinationDocumentIds(externalPmidDestinationDocumentIds);
		
		CitationEntry.Builder externalDoiCitationEntryBuilder = CitationEntry.newBuilder();
		externalDoiCitationEntryBuilder.setRawText("[17] N. Koblitz. Hyperelliptic cryptosystems. J. Cryptology, 1(3):139–150, 1989.");
		externalDoiCitationEntryBuilder.setPosition(17);
		Map<CharSequence, CharSequence> externalDoiDestinationDocumentIds = new HashMap<CharSequence, CharSequence>();
		externalDoiDestinationDocumentIds.put("doi", "10.1186/1753-6561-5-S6-P38");
		externalDoiDestinationDocumentIds.put("custom-id", "12345");
		externalDoiCitationEntryBuilder.setExternalDestinationDocumentIds(externalDoiDestinationDocumentIds);
		
		sortedCitations.add(CitationsActionBuilderModuleFactory.build(internalCitationEntryBuilder.build()
				,HBaseConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR));
		sortedCitations.add(CitationsActionBuilderModuleFactory.build(externalPmidCitationEntryBuilder.build()
				,HBaseConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR));
		sortedCitations.add(CitationsActionBuilderModuleFactory.build(externalDoiCitationEntryBuilder.build()
				,HBaseConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR));
		sortedCitations.add(CitationsActionBuilderModuleFactory.build(rawTextCitationEntryBuilder.build()
				,HBaseConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR));
		
		CitationsExtraInfoConverter converter = new CitationsExtraInfoConverter();
		String citationsXML = converter.serialize(sortedCitations);
		System.out.println(citationsXML);
		
//		checking deserialization
		SortedSet<BlobCitationEntry> deserializedCitations = (SortedSet<BlobCitationEntry>) converter.deserialize(citationsXML);
		Assert.assertEquals(sortedCitations.size(), deserializedCitations.size());
		Iterator<BlobCitationEntry> sortedIt = sortedCitations.iterator();
		Iterator<BlobCitationEntry> deserializedIt = deserializedCitations.iterator();
		for (int i=0; i<sortedCitations.size(); i++) {
			Assert.assertEquals(sortedIt.next(), deserializedIt.next());	
		}
	}
}
