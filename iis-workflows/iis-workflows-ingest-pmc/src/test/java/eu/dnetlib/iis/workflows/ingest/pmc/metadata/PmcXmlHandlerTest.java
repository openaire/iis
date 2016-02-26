package eu.dnetlib.iis.workflows.ingest.pmc.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Stack;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.junit.Test;
import org.xml.sax.XMLReader;

import eu.dnetlib.iis.ingest.pmc.metadata.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.ingest.pmc.metadata.schemas.ReferenceMetadata;
import eu.dnetlib.iis.workflows.ingest.pmc.metadata.PmcXmlHandler;

/**
 * {@link PmcXmlHandler} test class.
 * 
 * @author mhorst
 *
 */
public class PmcXmlHandlerTest {

	@Test
	public void testHasAmongParents() throws Exception {
		Stack<String> parents = new Stack<String>();
		parents.add("ref-list");
		parents.add("ref");
		parents.add("something");
		parents.add("name");

		assertTrue(
				PmcXmlHandler.hasAmongParents("surname", "surname", parents, "name", "something", "ref", "ref-list"));
		assertTrue(PmcXmlHandler.hasAmongParents("surname", "surname", parents, "name", "ref", "ref-list"));
		assertTrue(PmcXmlHandler.hasAmongParents("surname", "surname", parents, "name", "ref"));
		assertTrue(PmcXmlHandler.hasAmongParents("name", "name", parents, "name"));
		assertTrue(PmcXmlHandler.hasAmongParents("name", "name", parents, "ref"));
		assertTrue(PmcXmlHandler.hasAmongParents("name", "name", parents, "ref-list"));
		assertFalse(PmcXmlHandler.hasAmongParents("surname", "surname", parents, "ref", "name"));
		assertFalse(PmcXmlHandler.hasAmongParents("surname", "surname", parents, "ref-list", "ref"));
		assertFalse(PmcXmlHandler.hasAmongParents("name", "name", parents, "xxx"));
	}

	@Test
	public void testParsingJats10() throws Exception {
		String filePath = "/eu/dnetlib/iis/workflows/ingest/pmc/metadata/data/document_jats10.xml";
		InputStream inputStream = null;
		try {
			SAXParserFactory saxFactory = SAXParserFactory.newInstance();
			saxFactory.setValidating(false);
			SAXParser saxParser = saxFactory.newSAXParser();
			XMLReader reader = saxParser.getXMLReader();
			reader.setFeature("http://xml.org/sax/features/validation", false);
			reader.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
			reader.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);

			ExtractedDocumentMetadata.Builder metaBuilder = ExtractedDocumentMetadata.newBuilder();
			metaBuilder.setId("some-id");
			metaBuilder.setText("");
			PmcXmlHandler pmcXmlHandler = new PmcXmlHandler(metaBuilder);
			saxParser.parse(inputStream = PmcXmlHandler.class.getResourceAsStream(filePath), pmcXmlHandler);
			checkJats10Metadata(metaBuilder.build());
		} finally {
			if (inputStream != null) {
				inputStream.close();
			}
		}
	}

	@Test
	public void testParsingJats10NestedInOAI() throws Exception {
		String filePath = "/eu/dnetlib/iis/workflows/ingest/pmc/metadata/data/document_jats10_nested_in_oai.xml";
		InputStream inputStream = null;
		try {
			SAXParserFactory saxFactory = SAXParserFactory.newInstance();
			saxFactory.setValidating(false);
			SAXParser saxParser = saxFactory.newSAXParser();
			XMLReader reader = saxParser.getXMLReader();
			reader.setFeature("http://xml.org/sax/features/validation", false);
			reader.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
			reader.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);

			ExtractedDocumentMetadata.Builder metaBuilder = ExtractedDocumentMetadata.newBuilder();
			metaBuilder.setId("some-id");
			metaBuilder.setText("");
			PmcXmlHandler pmcXmlHandler = new PmcXmlHandler(metaBuilder);
			saxParser.parse(inputStream = PmcXmlHandler.class.getResourceAsStream(filePath), pmcXmlHandler);
			checkJats10Metadata(metaBuilder.build());
		} finally {
			if (inputStream != null) {
				inputStream.close();
			}
		}
	}

	private void checkJats10Metadata(ExtractedDocumentMetadata meta) throws Exception {
		assertEquals("research-article", meta.getEntityType());
		assertEquals("BMC Systems Biology", meta.getJournal());
		assertEquals(1, meta.getExternalIdentifiers().size());
		assertEquals("19943949", meta.getExternalIdentifiers().get("pmid"));
		assertEquals("111", meta.getPages().getStart());
		assertEquals("111", meta.getPages().getEnd());

		assertNotNull(meta.getReferences());
		assertEquals(34, meta.getReferences().size());
		// checking first reference
		assertEquals(1, meta.getReferences().get(0).getPosition().intValue());
		assertEquals(
				"Koonin, E. Comparative genomics, minimal gene-sets and the last universal common ancestor. Nat Rev Microbiol. 2003; 1 (2): 127-136",
				meta.getReferences().get(0).getText());
		assertEquals("Comparative genomics, minimal gene-sets and the last universal common ancestor",
				meta.getReferences().get(0).getBasicMetadata().getTitle());
		assertEquals(1, meta.getReferences().get(0).getBasicMetadata().getAuthors().size());
		assertEquals("Koonin, E", meta.getReferences().get(0).getBasicMetadata().getAuthors().get(0));
		assertEquals("127", meta.getReferences().get(0).getBasicMetadata().getPages().getStart());
		assertEquals("136", meta.getReferences().get(0).getBasicMetadata().getPages().getEnd());
		assertEquals("Nat Rev Microbiol", meta.getReferences().get(0).getBasicMetadata().getSource());
		assertEquals("1", meta.getReferences().get(0).getBasicMetadata().getVolume());
		assertEquals("2003", meta.getReferences().get(0).getBasicMetadata().getYear());
		assertEquals("2", meta.getReferences().get(0).getBasicMetadata().getIssue());
		assertEquals(2, meta.getReferences().get(0).getBasicMetadata().getExternalIds().size());
		assertEquals("10.1038/nrmicro751", meta.getReferences().get(0).getBasicMetadata().getExternalIds().get("doi"));
		assertEquals("15035042", meta.getReferences().get(0).getBasicMetadata().getExternalIds().get("pmid"));

		assertNotNull(meta.getAffiliations());
		assertEquals(2, meta.getAffiliations().size());
		// checking first affiliation
		assertEquals(
				"Graduate School of Bioscience and Biotechnology, Tokyo Institute of Technology, Nagatsuta-cho, Midori-ku, Yokohama 226-8501, Japan",
				meta.getAffiliations().get(0).getRawText());
		assertEquals("Graduate School of Bioscience and Biotechnology, Tokyo Institute of Technology",
				meta.getAffiliations().get(0).getOrganization());
		assertEquals("Japan", meta.getAffiliations().get(0).getCountryName());
		assertEquals("JP", meta.getAffiliations().get(0).getCountryCode());
		assertEquals("Nagatsuta-cho, Midori-ku, Yokohama 226-8501", meta.getAffiliations().get(0).getAddress());
	}

	@Test
	public void testParsingJats23NestedInOAI() throws Exception {
		String filePath = "/eu/dnetlib/iis/workflows/ingest/pmc/metadata/data/document_jats23_nested_in_oai.xml";
		InputStream inputStream = null;
		try {
			SAXParserFactory saxFactory = SAXParserFactory.newInstance();
			saxFactory.setValidating(false);
			SAXParser saxParser = saxFactory.newSAXParser();
			XMLReader reader = saxParser.getXMLReader();
			reader.setFeature("http://xml.org/sax/features/validation", false);
			reader.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
			reader.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);

			ExtractedDocumentMetadata.Builder metaBuilder = ExtractedDocumentMetadata.newBuilder();
			metaBuilder.setId("some-id");
			metaBuilder.setText("");
			PmcXmlHandler pmcXmlHandler = new PmcXmlHandler(metaBuilder);
			saxParser.parse(inputStream = PmcXmlHandler.class.getResourceAsStream(filePath), pmcXmlHandler);
			ExtractedDocumentMetadata meta = metaBuilder.build();
			assertEquals("research-article", meta.getEntityType());
			assertEquals("Frontiers in Neuroscience", meta.getJournal());
			assertNull(meta.getExternalIdentifiers());

			assertNull(meta.getPages());

			assertNotNull(meta.getReferences());
			assertEquals(130, meta.getReferences().size());
			// checking first reference
			assertEquals(1, meta.getReferences().get(0).getPosition().intValue());
			assertEquals(
					"Abrams D. A. Nicol T. Zecker S. Kraus N. (2009). "
							+ "Abnormal cortical processing of the syllable rate of speech in poor readers. "
							+ "J. Neurosci. 29, 7686–7693. 10.1523/JNEUROSCI.5242-08.2009 19535580",
					meta.getReferences().get(0).getText());
			assertEquals("Abnormal cortical processing of the syllable rate of speech in poor readers",
					meta.getReferences().get(0).getBasicMetadata().getTitle());
			assertEquals(4, meta.getReferences().get(0).getBasicMetadata().getAuthors().size());
			assertEquals("Abrams, D. A.", meta.getReferences().get(0).getBasicMetadata().getAuthors().get(0));
			assertEquals("Nicol, T.", meta.getReferences().get(0).getBasicMetadata().getAuthors().get(1));
			assertEquals("Zecker, S.", meta.getReferences().get(0).getBasicMetadata().getAuthors().get(2));
			assertEquals("Kraus, N.", meta.getReferences().get(0).getBasicMetadata().getAuthors().get(3));
			assertEquals("7686", meta.getReferences().get(0).getBasicMetadata().getPages().getStart());
			assertEquals("7693", meta.getReferences().get(0).getBasicMetadata().getPages().getEnd());
			assertEquals("J. Neurosci", meta.getReferences().get(0).getBasicMetadata().getSource());
			assertEquals("29", meta.getReferences().get(0).getBasicMetadata().getVolume());
			assertEquals("2009", meta.getReferences().get(0).getBasicMetadata().getYear());
			assertNull(meta.getReferences().get(0).getBasicMetadata().getIssue());
			assertEquals(2, meta.getReferences().get(0).getBasicMetadata().getExternalIds().size());
			assertEquals("10.1523/JNEUROSCI.5242-08.2009",
					meta.getReferences().get(0).getBasicMetadata().getExternalIds().get("doi"));
			assertEquals("19535580", meta.getReferences().get(0).getBasicMetadata().getExternalIds().get("pmid"));

			assertNotNull(meta.getAffiliations());
			assertEquals(6, meta.getAffiliations().size());
			// checking first affiliation
			assertEquals(
					"Auditory Neuroscience Laboratory, www.brainvolts.northwestern.edu, Northwestern University Evanston, IL, USA",
					meta.getAffiliations().get(0).getRawText());
			assertEquals(
					"Auditory Neuroscience Laboratory, www.brainvolts.northwestern.edu, Northwestern University Evanston",
					meta.getAffiliations().get(0).getOrganization());
			assertEquals("USA", meta.getAffiliations().get(0).getCountryName());
			assertEquals("US", meta.getAffiliations().get(0).getCountryCode());
			assertEquals("IL", meta.getAffiliations().get(0).getAddress());

		} finally {
			if (inputStream != null) {
				inputStream.close();
			}
		}
	}

	@Test
	public void testParsingLargeFile() throws Exception {
		String filePath = "/eu/dnetlib/iis/workflows/ingest/pmc/metadata/data/od_______908__365a50343d53774f68fa13800349d372.xml";
		InputStream inputStream = null;
		try {
			SAXParserFactory saxFactory = SAXParserFactory.newInstance();
			saxFactory.setValidating(false);
			SAXParser saxParser = saxFactory.newSAXParser();
			XMLReader reader = saxParser.getXMLReader();
			reader.setFeature("http://xml.org/sax/features/validation", false);
			reader.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
			reader.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);

			ExtractedDocumentMetadata.Builder metaBuilder = ExtractedDocumentMetadata.newBuilder();
			metaBuilder.setId("some-id");
			metaBuilder.setText("");
			PmcXmlHandler pmcXmlHandler = new PmcXmlHandler(metaBuilder);
			saxParser.parse(inputStream = PmcXmlHandler.class.getResourceAsStream(filePath), pmcXmlHandler);
			ExtractedDocumentMetadata meta = metaBuilder.build();
			assertEquals("ZooKeys", meta.getJournal());
			assertEquals("1", meta.getPages().getStart());
			assertEquals("972", meta.getPages().getEnd());
			assertNotNull(meta.getReferences());
			assertEquals(2643, meta.getReferences().size());
		} finally {
			if (inputStream != null) {
				inputStream.close();
			}
		}
	}

	@Test
	public void testParsingAffiliation() throws Exception {
		String filePath = "/eu/dnetlib/iis/workflows/ingest/pmc/metadata/data/document_with_affiliations.xml";
		InputStream inputStream = null;
		try {
			SAXParserFactory saxFactory = SAXParserFactory.newInstance();
			saxFactory.setValidating(false);
			SAXParser saxParser = saxFactory.newSAXParser();
			XMLReader reader = saxParser.getXMLReader();
			reader.setFeature("http://xml.org/sax/features/validation", false);
			reader.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
			reader.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);

			ExtractedDocumentMetadata.Builder metaBuilder = ExtractedDocumentMetadata.newBuilder();
			metaBuilder.setId("some-id");
			metaBuilder.setText("");
			PmcXmlHandler pmcXmlHandler = new PmcXmlHandler(metaBuilder);
			saxParser.parse(inputStream = PmcXmlHandler.class.getResourceAsStream(filePath), pmcXmlHandler);
			ExtractedDocumentMetadata meta = metaBuilder.build();

			assertNotNull(meta.getAffiliations());
			assertEquals(5, meta.getAffiliations().size());

			assertEquals("US", meta.getAffiliations().get(0).getCountryCode());
			assertEquals("National Center for Biotechnology Information, National Library of Medicine, NIH",
					meta.getAffiliations().get(0).getOrganization());
			assertEquals("US", meta.getAffiliations().get(1).getCountryCode());
			assertEquals("Consolidated Safety Services", meta.getAffiliations().get(1).getOrganization());
			assertEquals(null, meta.getAffiliations().get(2).getCountryCode());
			assertEquals(
					"National Center for Biotechnology Information, National Library of Medicine, National Institutes of Health",
					meta.getAffiliations().get(2).getOrganization());
			assertEquals("JP", meta.getAffiliations().get(3).getCountryCode());
			assertEquals("Graduate School of Bioscience and Biotechnology, Tokyo Institute of Technology",
					meta.getAffiliations().get(3).getOrganization());
			assertEquals("JP", meta.getAffiliations().get(4).getCountryCode());
			assertEquals("Graduate School of Information Science, Nagoya University",
					meta.getAffiliations().get(4).getOrganization());

		} finally {
			if (inputStream != null) {
				inputStream.close();
			}
		}
	}

	@Test
	public void testSingleRefParsing() throws Exception {
		String filePath = "/eu/dnetlib/iis/workflows/ingest/pmc/metadata/data/single-ref-document.xml";
		InputStream inputStream = null;
		try {
			SAXParserFactory saxFactory = SAXParserFactory.newInstance();
			saxFactory.setValidating(false);
			SAXParser saxParser = saxFactory.newSAXParser();
			XMLReader reader = saxParser.getXMLReader();
			reader.setFeature("http://xml.org/sax/features/validation", false);
			reader.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
			reader.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);

			ExtractedDocumentMetadata.Builder metaBuilder = ExtractedDocumentMetadata.newBuilder();
			metaBuilder.setId("some-id");
			metaBuilder.setText("");
			PmcXmlHandler pmcXmlHandler = new PmcXmlHandler(metaBuilder);
			saxParser.parse(inputStream = PmcXmlHandler.class.getResourceAsStream(filePath), pmcXmlHandler);
			ExtractedDocumentMetadata meta = metaBuilder.build();
			assertNotNull(meta.getReferences());
			assertEquals(1, meta.getReferences().size());
			ReferenceMetadata refMeta = meta.getReferences().get(0);
			assertEquals(
					"2 Jemal A, Bray F, Center MM, Ferlay J, Ward E, et al (2011) Global cancer statistics. CA Cancer J Clin 61: 69-90 21296855",
					refMeta.getText());
			assertEquals(new Integer(1), refMeta.getPosition());
			assertEquals(Arrays.asList(new String[] { "Jemal, A", "Bray, F", "Center, MM", "Ferlay, J", "Ward, E" }),
					refMeta.getBasicMetadata().getAuthors());
			assertEquals("Global cancer statistics", refMeta.getBasicMetadata().getTitle());
			assertEquals("CA Cancer J Clin", refMeta.getBasicMetadata().getSource());
			assertEquals("61", refMeta.getBasicMetadata().getVolume());
			assertNull(refMeta.getBasicMetadata().getIssue());
			assertEquals("2011", refMeta.getBasicMetadata().getYear());
			assertEquals("69", refMeta.getBasicMetadata().getPages().getStart());
			assertEquals("90", refMeta.getBasicMetadata().getPages().getEnd());
			assertEquals(1, refMeta.getBasicMetadata().getExternalIds().size());
			assertEquals("21296855", refMeta.getBasicMetadata().getExternalIds().get("pmid"));
		} finally {
			if (inputStream != null) {
				inputStream.close();
			}
		}
	}

	@Test
	public void testMixedTitleParsing() throws Exception {
		// files causing parsing problems
		String filePath = "/eu/dnetlib/iis/workflows/ingest/pmc/metadata/data/od_______908__0451fa1ded79a63729296731e53335c0.xml";
		InputStream inputStream = null;
		try {
			SAXParserFactory saxFactory = SAXParserFactory.newInstance();
			saxFactory.setValidating(false);
			SAXParser saxParser = saxFactory.newSAXParser();
			XMLReader reader = saxParser.getXMLReader();
			reader.setFeature("http://xml.org/sax/features/validation", false);
			reader.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
			reader.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);

			ExtractedDocumentMetadata.Builder metaBuilder = ExtractedDocumentMetadata.newBuilder();
			metaBuilder.setId("some-id");
			metaBuilder.setText("");
			PmcXmlHandler pmcXmlHandler = new PmcXmlHandler(metaBuilder);
			saxParser.parse(inputStream = PmcXmlHandler.class.getResourceAsStream(filePath), pmcXmlHandler);
			ExtractedDocumentMetadata meta = metaBuilder.build();
			assertNotNull(meta.getReferences());
			ReferenceMetadata refMeta = meta.getReferences().get(12);
			assertEquals(
					"13 Shearer KD, Silverstein J, Plisetskaya EM (1997) Role of adiposity in food intake control of juvenile chinook salmon (Oncorhynchus tshawytscha). Comp Biochem Physiol A 118: 1209–1215",
					refMeta.getText());
			assertEquals(new Integer(13), refMeta.getPosition());
			assertEquals(Arrays.asList(new String[] { "Shearer, KD", "Silverstein, J", "Plisetskaya, EM" }),
					refMeta.getBasicMetadata().getAuthors());
			assertEquals(
					"Role of adiposity in food intake control of juvenile chinook salmon (Oncorhynchus tshawytscha)",
					refMeta.getBasicMetadata().getTitle());
			assertEquals("Comp Biochem Physiol A", refMeta.getBasicMetadata().getSource());
			assertEquals("118", refMeta.getBasicMetadata().getVolume());
			assertNull(refMeta.getBasicMetadata().getIssue());
			assertEquals("1997", refMeta.getBasicMetadata().getYear());
			assertEquals("1209", refMeta.getBasicMetadata().getPages().getStart());
			assertEquals("1215", refMeta.getBasicMetadata().getPages().getEnd());
			assertEquals(0, refMeta.getBasicMetadata().getExternalIds().size());

		} finally {
			if (inputStream != null) {
				inputStream.close();
			}
		}
	}

	@Test
	public void testElementCitation() throws Exception {
		// files causing parsing problems
		String filePath = "/eu/dnetlib/iis/workflows/ingest/pmc/metadata/data/od_______908__0452195ccf851072fd097fc49bfbb9da.xml";
		InputStream inputStream = null;
		try {
			SAXParserFactory saxFactory = SAXParserFactory.newInstance();
			saxFactory.setValidating(false);
			SAXParser saxParser = saxFactory.newSAXParser();
			XMLReader reader = saxParser.getXMLReader();
			reader.setFeature("http://xml.org/sax/features/validation", false);
			reader.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
			reader.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);

			ExtractedDocumentMetadata.Builder metaBuilder = ExtractedDocumentMetadata.newBuilder();
			metaBuilder.setId("some-id");
			metaBuilder.setText("");
			PmcXmlHandler pmcXmlHandler = new PmcXmlHandler(metaBuilder);
			saxParser.parse(inputStream = PmcXmlHandler.class.getResourceAsStream(filePath), pmcXmlHandler);
			ExtractedDocumentMetadata meta = metaBuilder.build();
			assertNotNull(meta.getReferences());

			ReferenceMetadata refMeta = meta.getReferences().get(0);
			assertEquals("Guzman, MG, Kouri, G. Dengue: an update.. Lancet Infect Dis. 2002; 2: 33-42",
					refMeta.getText());
			assertEquals(new Integer(1), refMeta.getPosition());
			assertEquals(Arrays.asList(new String[] { "Guzman, MG", "Kouri, G" }),
					refMeta.getBasicMetadata().getAuthors());
			assertEquals("Dengue: an update.", refMeta.getBasicMetadata().getTitle());
			assertEquals("Lancet Infect Dis", refMeta.getBasicMetadata().getSource());
			assertEquals("2", refMeta.getBasicMetadata().getVolume());
			assertNull(refMeta.getBasicMetadata().getIssue());
			assertEquals("2002", refMeta.getBasicMetadata().getYear());
			assertEquals("33", refMeta.getBasicMetadata().getPages().getStart());
			assertEquals("42", refMeta.getBasicMetadata().getPages().getEnd());
			assertEquals(1, refMeta.getBasicMetadata().getExternalIds().size());
			assertEquals("11892494", refMeta.getBasicMetadata().getExternalIds().get("pmid"));
		} finally {
			if (inputStream != null) {
				inputStream.close();
			}
		}
	}
}
