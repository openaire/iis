package eu.dnetlib.iis.wf.ingest.pmc.metadata;

import static eu.dnetlib.iis.wf.ingest.pmc.metadata.AssertExtractedDocumentMetadata.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.Reader;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.ingest.pmc.metadata.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.ingest.pmc.metadata.schemas.ReferenceBasicMetadata;
import eu.dnetlib.iis.ingest.pmc.metadata.schemas.ReferenceMetadata;

/**
 * {@link JatsXmlHandler} test class.
 * 
 * @author mhorst
 *
 */
public class JatsXmlHandlerTest {

	private Reader fileReader;
	private SAXParser saxParser;
	private ExtractedDocumentMetadata.Builder metaBuilder;
	private JatsXmlHandler jatsXmlHandler;

	static final String xmlResourcesRootClassPath = "/eu/dnetlib/iis/wf/ingest/pmc/metadata/data/";

	@Before
	public void init() throws Exception {
		// initializing sax parser
		SAXParserFactory saxFactory = SAXParserFactory.newInstance();
		saxFactory.setValidating(false);
		saxParser = saxFactory.newSAXParser();
		XMLReader reader = saxParser.getXMLReader();
		reader.setFeature("http://xml.org/sax/features/validation", false);
		reader.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
		reader.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
		// initializing metadata builder with required fields
		metaBuilder = ExtractedDocumentMetadata.newBuilder();
		metaBuilder.setId("some-id");
		metaBuilder.setText("");
		jatsXmlHandler = new JatsXmlHandler(metaBuilder);
	}

	@After
	public void clean() throws Exception {
		if (fileReader != null) {
			fileReader.close();
		}
	}


	@Test
	public void testParsingJats10() throws Exception {
		fileReader = ClassPathResourceProvider
				.getResourceReader(xmlResourcesRootClassPath + "document_jats10.xml");
		InputSource inputSource = new InputSource(fileReader);
		saxParser.parse(inputSource, jatsXmlHandler);
		checkJats10Metadata(metaBuilder.build());
	}

	@Test
	public void testParsingJats10NestedInOAI() throws Exception {
		fileReader = ClassPathResourceProvider
				.getResourceReader(xmlResourcesRootClassPath + "document_jats10_nested_in_oai.xml");
		InputSource inputSource = new InputSource(fileReader);
		saxParser.parse(inputSource, jatsXmlHandler);
		checkJats10Metadata(metaBuilder.build());
	}

	@Test
	public void testParsingJats23NestedInOAI() throws Exception {
		fileReader = ClassPathResourceProvider
				.getResourceReader(xmlResourcesRootClassPath + "document_jats23_nested_in_oai.xml");
		InputSource inputSource = new InputSource(fileReader);
		saxParser.parse(inputSource, jatsXmlHandler);
		ExtractedDocumentMetadata meta = metaBuilder.build();
		assertEquals("research-article", meta.getEntityType());
		assertEquals("Frontiers in Neuroscience", meta.getJournal());
		assertEquals(1, meta.getExternalIdentifiers().size());
		assertEquals("10.3389/fnins.2014.00351", meta.getExternalIdentifiers().get("doi"));
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
		ReferenceBasicMetadata basicMeta = meta.getReferences().get(0).getBasicMetadata();
		assertEquals("Abnormal cortical processing of the syllable rate of speech in poor readers",
				basicMeta.getTitle());
		assertEquals(4, basicMeta.getAuthors().size());
		assertEquals("Abrams, D. A.", basicMeta.getAuthors().get(0));
		assertEquals("Nicol, T.", basicMeta.getAuthors().get(1));
		assertEquals("Zecker, S.", basicMeta.getAuthors().get(2));
		assertEquals("Kraus, N.", basicMeta.getAuthors().get(3));
		assertEquals("7686", basicMeta.getPages().getStart());
		assertEquals("7693", basicMeta.getPages().getEnd());
		assertEquals("J. Neurosci", basicMeta.getSource());
		assertEquals("29", basicMeta.getVolume());
		assertEquals("2009", basicMeta.getYear());
		assertNull(basicMeta.getIssue());
		assertEquals(2, basicMeta.getExternalIds().size());
		assertEquals("10.1523/JNEUROSCI.5242-08.2009", basicMeta.getExternalIds().get("doi"));
		assertEquals("19535580", basicMeta.getExternalIds().get("pmid"));

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
	}

	@Test
	public void testParsingLargeFile() throws Exception {
		fileReader = ClassPathResourceProvider
				.getResourceReader(xmlResourcesRootClassPath + "od_______908__365a50343d53774f68fa13800349d372.xml");
		InputSource inputSource = new InputSource(fileReader);
		saxParser.parse(inputSource, jatsXmlHandler);
		ExtractedDocumentMetadata meta = metaBuilder.build();
		assertEquals("ZooKeys", meta.getJournal());
		assertEquals("1", meta.getPages().getStart());
		assertEquals("972", meta.getPages().getEnd());
		assertNotNull(meta.getReferences());
		assertEquals(2643, meta.getReferences().size());
	}

	@Test
	public void testParsingAuthorsWithAffiliation() throws Exception {
		fileReader = ClassPathResourceProvider
				.getResourceReader(xmlResourcesRootClassPath + "document_with_affiliations.xml");
		InputSource inputSource = new InputSource(fileReader);
		saxParser.parse(inputSource, jatsXmlHandler);
		ExtractedDocumentMetadata meta = metaBuilder.build();

		assertNotNull(meta.getAffiliations());
		assertEquals(5, meta.getAffiliations().size());

		assertEquals("US", meta.getAffiliations().get(0).getCountryCode());
		assertEquals("National Center for Biotechnology Information, National Library of Medicine, NIH",
				meta.getAffiliations().get(0).getOrganization());
		assertEquals("US", meta.getAffiliations().get(1).getCountryCode());
		assertEquals("Consolidated Safety Services", meta.getAffiliations().get(1).getOrganization());
		assertEquals("US", meta.getAffiliations().get(2).getCountryCode());
		assertEquals(
				"National Center for Biotechnology Information, National Library of Medicine, National Institutes of Health",
				meta.getAffiliations().get(2).getOrganization());
		assertEquals("JP", meta.getAffiliations().get(3).getCountryCode());
		assertEquals("Graduate School of Bioscience and Biotechnology, Tokyo Institute of Technology",
				meta.getAffiliations().get(3).getOrganization());
		assertEquals("JP", meta.getAffiliations().get(4).getCountryCode());
		assertEquals("Graduate School of Information Science, Nagoya University",
				meta.getAffiliations().get(4).getOrganization());
		
		
		assertNotNull(meta.getAuthors());
		assertEquals(8, meta.getAuthors().size());
		
		assertAuthor(meta.getAuthors().get(0), "Tanabe, Lorraine", 0);
		assertAuthor(meta.getAuthors().get(1), "Thom, Lynne H.", 1);
		assertAuthor(meta.getAuthors().get(2), "Marth, Gabor T.", 2);
		assertAuthor(meta.getAuthors().get(3), "Czabarka, Eva", 2);
		assertAuthor(meta.getAuthors().get(4), "Murvai, Janos", 2);
		assertAuthor(meta.getAuthors().get(5), "Sherry, Stephen T.", 2);
		assertAuthor(meta.getAuthors().get(6), "Azuma, Yusuke", 3);
		assertAuthor(meta.getAuthors().get(7), "Ota, Motonori", 4);
		
	}

	@Test
	public void testSingleRefParsing() throws Exception {
		fileReader = ClassPathResourceProvider
				.getResourceReader(xmlResourcesRootClassPath + "single-ref-document.xml");
		InputSource inputSource = new InputSource(fileReader);
		saxParser.parse(inputSource, jatsXmlHandler);
		ExtractedDocumentMetadata meta = metaBuilder.build();
		assertNotNull(meta.getReferences());
		assertEquals(1, meta.getReferences().size());
		ReferenceMetadata refMeta = meta.getReferences().get(0);
		assertEquals(
				"2 Jemal A, Bray F, Center MM, Ferlay J, Ward E, et al (2011) Global cancer statistics. CA Cancer J Clin 61: 69-90 21296855",
				refMeta.getText());
		assertEquals(Integer.valueOf(1), refMeta.getPosition());
		assertEquals(Lists.newArrayList("Jemal, A", "Bray, F", "Center, MM", "Ferlay, J", "Ward, E"),
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
	}

	@Test
	public void testMixedTitleParsing() throws Exception {
		// files causing parsing problems
		fileReader = ClassPathResourceProvider
				.getResourceReader(xmlResourcesRootClassPath + "od_______908__0451fa1ded79a63729296731e53335c0.xml");
		InputSource inputSource = new InputSource(fileReader);
		saxParser.parse(inputSource, jatsXmlHandler);
		ExtractedDocumentMetadata meta = metaBuilder.build();
		assertNotNull(meta.getReferences());
		ReferenceMetadata refMeta = meta.getReferences().get(12);
		assertEquals(
				"13 Shearer KD, Silverstein J, Plisetskaya EM (1997) Role of adiposity in food intake control of juvenile chinook salmon (Oncorhynchus tshawytscha). Comp Biochem Physiol A 118: 1209–1215",
				refMeta.getText());
		assertEquals(Integer.valueOf(13), refMeta.getPosition());
		assertEquals(Lists.newArrayList("Shearer, KD", "Silverstein, J", "Plisetskaya, EM"),
				refMeta.getBasicMetadata().getAuthors());
		assertEquals("Role of adiposity in food intake control of juvenile chinook salmon (Oncorhynchus tshawytscha)",
				refMeta.getBasicMetadata().getTitle());
		assertEquals("Comp Biochem Physiol A", refMeta.getBasicMetadata().getSource());
		assertEquals("118", refMeta.getBasicMetadata().getVolume());
		assertNull(refMeta.getBasicMetadata().getIssue());
		assertEquals("1997", refMeta.getBasicMetadata().getYear());
		assertEquals("1209", refMeta.getBasicMetadata().getPages().getStart());
		assertEquals("1215", refMeta.getBasicMetadata().getPages().getEnd());
		assertEquals(0, refMeta.getBasicMetadata().getExternalIds().size());
	}

	@Test
	public void testElementCitation() throws Exception {
		// files causing parsing problems
		fileReader = ClassPathResourceProvider
				.getResourceReader(xmlResourcesRootClassPath + "od_______908__0452195ccf851072fd097fc49bfbb9da.xml");
		InputSource inputSource = new InputSource(fileReader);
		saxParser.parse(inputSource, jatsXmlHandler);
		ExtractedDocumentMetadata meta = metaBuilder.build();
		assertNotNull(meta.getReferences());

		ReferenceMetadata refMeta = meta.getReferences().get(0);
		assertEquals("Guzman, MG, Kouri, G. Dengue: an update.. Lancet Infect Dis. 2002; 2: 33-42", refMeta.getText());
		assertEquals(Integer.valueOf(1), refMeta.getPosition());
		assertEquals(Lists.newArrayList("Guzman, MG", "Kouri, G"), refMeta.getBasicMetadata().getAuthors());
		assertEquals("Dengue: an update.", refMeta.getBasicMetadata().getTitle());
		assertEquals("Lancet Infect Dis", refMeta.getBasicMetadata().getSource());
		assertEquals("2", refMeta.getBasicMetadata().getVolume());
		assertNull(refMeta.getBasicMetadata().getIssue());
		assertEquals("2002", refMeta.getBasicMetadata().getYear());
		assertEquals("33", refMeta.getBasicMetadata().getPages().getStart());
		assertEquals("42", refMeta.getBasicMetadata().getPages().getEnd());
		assertEquals(1, refMeta.getBasicMetadata().getExternalIds().size());
		assertEquals("11892494", refMeta.getBasicMetadata().getExternalIds().get("pmid"));
	}

	private void checkJats10Metadata(ExtractedDocumentMetadata meta) throws Exception {
		assertEquals("research-article", meta.getEntityType());
		assertEquals("BMC Systems Biology", meta.getJournal());
		assertEquals(4, meta.getExternalIdentifiers().size());
		assertEquals("19943949", meta.getExternalIdentifiers().get("pmid"));
		assertEquals("1752-0509-3-111", meta.getExternalIdentifiers().get("publisher-id"));
		assertEquals("2789071", meta.getExternalIdentifiers().get("pmc"));
		assertEquals("10.1186/1752-0509-3-111", meta.getExternalIdentifiers().get("doi"));
		assertEquals("111", meta.getPages().getStart());
		assertEquals("111", meta.getPages().getEnd());

		assertNotNull(meta.getReferences());
		assertEquals(34, meta.getReferences().size());
		// checking first reference
		assertEquals(1, meta.getReferences().get(0).getPosition().intValue());
		assertEquals(
				"Koonin, E. Comparative genomics, minimal gene-sets and the last universal common ancestor. Nat Rev Microbiol. 2003; 1 (2): 127-136",
				meta.getReferences().get(0).getText());
		ReferenceBasicMetadata basicMeta = meta.getReferences().get(0).getBasicMetadata();
		assertEquals("Comparative genomics, minimal gene-sets and the last universal common ancestor",
				basicMeta.getTitle());
		assertEquals(1, basicMeta.getAuthors().size());
		assertEquals("Koonin, E", basicMeta.getAuthors().get(0));
		assertEquals("127", basicMeta.getPages().getStart());
		assertEquals("136", basicMeta.getPages().getEnd());
		assertEquals("Nat Rev Microbiol", basicMeta.getSource());
		assertEquals("1", basicMeta.getVolume());
		assertEquals("2003", basicMeta.getYear());
		assertEquals("2", basicMeta.getIssue());
		assertEquals(2, basicMeta.getExternalIds().size());
		assertEquals("10.1038/nrmicro751", basicMeta.getExternalIds().get("doi"));
		assertEquals("15035042", basicMeta.getExternalIds().get("pmid"));

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
}
