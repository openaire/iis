package eu.dnetlib.iis.wf.ingest.pmc.metadata;

import static eu.dnetlib.iis.wf.ingest.pmc.metadata.AssertExtractedDocumentMetadata.assertAffiliation;
import static eu.dnetlib.iis.wf.ingest.pmc.metadata.AssertExtractedDocumentMetadata.assertAuthor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.util.List;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.xml.sax.XMLReader;

import com.google.common.collect.Maps;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.ingest.pmc.metadata.schemas.Affiliation;
import eu.dnetlib.iis.ingest.pmc.metadata.schemas.Author;
import eu.dnetlib.iis.ingest.pmc.metadata.schemas.ExtractedDocumentMetadata;

/**
 * @author madryk
 */
public class ArticleMetaXmlHandlerTest {

    private final static String XML_BASE_PATH = ClassPathResourceProvider
            .getResourcePath("eu/dnetlib/iis/wf/ingest/pmc/metadata/data/articlemeta");
    
    private ArticleMetaXmlHandler articleMetaXmlHandler;
    
    private SAXParser saxParser;
    
    private ExtractedDocumentMetadata.Builder metaBuilder;
    
    
    @BeforeEach
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
        metaBuilder.setExternalIdentifiers(Maps.newHashMap());
        metaBuilder.setEntityType("");
        
        articleMetaXmlHandler = new ArticleMetaXmlHandler(metaBuilder);
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void testAuthorsWithXRefAffiliation() throws Exception {
        
        // given
        
        File xmlFile = new File(XML_BASE_PATH + "/authors_with_aff_refs.xml");
        
        
        // execute
        
        saxParser.parse(xmlFile, articleMetaXmlHandler);
        
        ExtractedDocumentMetadata metadata = metaBuilder.build();
        
        
        // assert
        
        List<Affiliation> affiliations = metadata.getAffiliations();
        assertEquals(2, affiliations.size());
        
        assertAffiliation(affiliations.get(0), "Graduate School of Bioscience and Biotechnology, Tokyo Institute of Technology",
                "Nagatsuta-cho, Midori-ku, Yokohama 226-8501", "JP", "Japan",
                "Graduate School of Bioscience and Biotechnology, Tokyo Institute of Technology, Nagatsuta-cho, Midori-ku, Yokohama 226-8501, Japan");
        assertAffiliation(affiliations.get(1), "Graduate School of Information Science, Nagoya University", 
                "Furo-cho, Chikusa-ku, Nagoya 464-8601", "JP", "Japan",
                "Graduate School of Information Science, Nagoya University, Furo-cho, Chikusa-ku, Nagoya 464-8601, Japan");
        
        
        List<Author> authors = metadata.getAuthors();
        assertEquals(2, authors.size());
        
        assertAuthor(authors.get(0), "Azuma, Yusuke", 0);
        assertAuthor(authors.get(1), "Ota, Motonori", 1);
        
    }
    
    @Test
    public void testAuthorsWithAffiliationInContrib() throws Exception {
        
        // given
        
        File xmlFile = new File(XML_BASE_PATH + "/authors_with_aff_in_contrib.xml");
        
        
        // execute
        
        saxParser.parse(xmlFile, articleMetaXmlHandler);
        
        ExtractedDocumentMetadata metadata = metaBuilder.build();
        
        
        // assert
        
        List<Affiliation> affiliations = metadata.getAffiliations();
        assertEquals(2, affiliations.size());
        
        assertAffiliation(affiliations.get(0), "National Center for Biotechnology Information, National Library of Medicine, NIH",
                "8600 Rockville Pike, Bethesda, MD", "US", "USA",
                "National Center for Biotechnology Information, National Library of Medicine, NIH, 8600 Rockville Pike, Bethesda, MD, USA");
        assertAffiliation(affiliations.get(1), "Consolidated Safety Services", 
                "10335 Democracy Lane, Suite 202, Fairfax, VA", "US", "USA",
                "Consolidated Safety Services, 10335 Democracy Lane, Suite 202, Fairfax, VA, USA");
        
        
        List<Author> authors = metadata.getAuthors();
        assertEquals(2, authors.size());
        
        assertAuthor(authors.get(0), "Tanabe, Lorraine", 0);
        assertAuthor(authors.get(1), "Thom, Lynne H.", 1);
        
    }
    
    @Test
    public void testAuthorsWithAffiliationInContribGroup() throws Exception {
        
        // given
        File xmlFile = new File(XML_BASE_PATH + "/authors_with_aff_in_contrib_group.xml");
        
        
        // execute
        
        saxParser.parse(xmlFile, articleMetaXmlHandler);
        
        ExtractedDocumentMetadata metadata = metaBuilder.build();
        
        
        // assert
        
        List<Affiliation> affiliations = metadata.getAffiliations();
        assertEquals(2, affiliations.size());
        
        assertAffiliation(affiliations.get(0), "National Center for Biotechnology Information, National Library of Medicine, NIH",
                "8600 Rockville Pike, Bethesda, MD", "US", "USA",
                "National Center for Biotechnology Information, National Library of Medicine, NIH, 8600 Rockville Pike, Bethesda, MD, USA");
        assertAffiliation(affiliations.get(1), "Consolidated Safety Services", 
                "10335 Democracy Lane, Suite 202, Fairfax, VA", "US", "USA",
                "Consolidated Safety Services, 10335 Democracy Lane, Suite 202, Fairfax, VA, USA");
        
        
        List<Author> authors = metadata.getAuthors();
        assertEquals(4, authors.size());
        
        assertAuthor(authors.get(0), "Tanabe, Lorraine", 0);
        assertAuthor(authors.get(1), "Xie, Natalie", 0);
        assertAuthor(authors.get(2), "Thom, Lynne H.", 1);
        assertAuthor(authors.get(3), "Matten, Wayne", 1);
        
    }
    
    
    @Test
    public void testAuthorsWithEncodedCharacters() throws Exception {
        
        // given
        File xmlFile = new File(XML_BASE_PATH + "/authors_with_encoded_characters.xml");
        
        
        // execute
        
        saxParser.parse(xmlFile, articleMetaXmlHandler);
        
        ExtractedDocumentMetadata metadata = metaBuilder.build();
        
        
        // assert
        
        List<Author> authors = metadata.getAuthors();
        assertEquals(3, authors.size());
        
        assertAuthor(authors.get(0), "Ramírez-Romero, Miguel A.");
        assertAuthor(authors.get(1), "González, Víctor");
        assertAuthor(authors.get(2), "Dávila, Guillermo");
        
    }

    @Test
    public void testNestedContributorsFromSpringer() throws Exception {
        // given
        File xmlFile = new File(XML_BASE_PATH + "/nested_contributors_from_springer.xml");
        
        // execute
        saxParser.parse(xmlFile, articleMetaXmlHandler);
        ExtractedDocumentMetadata metadata = metaBuilder.build();
        
        // assert
        
        List<Author> authors = metadata.getAuthors();
        assertEquals(2, authors.size());
        
        assertAuthor(authors.get(0), "Niemi, Mari E. K.", 0);
        assertAuthor(authors.get(1), "Karjalainen, Juha", 1);
    }
    
    @Test
    public void testComplexAffiliationsFromSpringer() throws Exception {
        // given
        File xmlFile = new File(XML_BASE_PATH + "/complex_affiliations_from_springer.xml");
        
        // execute
        saxParser.parse(xmlFile, articleMetaXmlHandler);
        ExtractedDocumentMetadata metadata = metaBuilder.build();
        
        // assert
        
        List<Affiliation> affs = metadata.getAffiliations();
        
        assertEquals(7, affs.size());
        
        assertEquals("DE", affs.get(0).getCountryCode());
        assertEquals("Germany", affs.get(0).getCountryName());
        assertEquals("Max-Planck-Institute for Immunobiology", affs.get(0).getOrganization());
        assertEquals("Schillhof 5, 79110 Freiburg", affs.get(0).getAddress());
        assertEquals("Max-Planck-Institute for Immunobiology, Schillhof 5, 79110 Freiburg, Germany", affs.get(0).getRawText());
        
        assertNull(affs.get(1).getCountryCode());
        assertNull(affs.get(1).getCountryName());
        assertEquals("Laboratory of Toxicology, Department of Pharmacological Sciences, University of Milan", affs.get(1).getOrganization());
        assertEquals("Milan", affs.get(1).getAddress());
        assertEquals("Laboratory of Toxicology, Department of Pharmacological Sciences, University of Milan, Milan", affs.get(1).getRawText());
        
        assertEquals("DE", affs.get(2).getCountryCode());
        assertEquals("Germany", affs.get(2).getCountryName());
        assertEquals("Proteome Sciences R&D GmbH & Co. KG", affs.get(2).getOrganization());
        assertEquals("Frankfurt", affs.get(2).getAddress());
        assertEquals("Proteome Sciences R&D GmbH & Co. KG, Frankfurt, Germany", affs.get(2).getRawText());
        
        assertEquals("DE", affs.get(3).getCountryCode());
        assertEquals("Germany", affs.get(3).getCountryName());
        assertEquals("Department of Dermatology, University Medical Centre", affs.get(3).getOrganization());
        assertEquals("Mannheim", affs.get(3).getAddress());
        assertEquals("Department of Dermatology, University Medical Centre, Mannheim, Germany", affs.get(3).getRawText());
        
        assertEquals("DK", affs.get(4).getCountryCode());
        assertEquals("Denmark", affs.get(4).getCountryName());
        assertEquals("Novozymes AS", affs.get(4).getOrganization());
        assertEquals("Bagsvaerd", affs.get(4).getAddress());
        assertEquals("Novozymes AS, Bagsvaerd, Denmark", affs.get(4).getRawText());
        
        // this is a mixed case: having institution and addr-line elements but without any specific content-type, also country needs to be discovered
        assertEquals("MX", affs.get(5).getCountryCode());
        assertEquals("México", affs.get(5).getCountryName());
        assertEquals("Programa de Genómica Evolutiva, Centro de Ciencias Genómicas, Universidad Nacional Autónoma de México", affs.get(5).getOrganization());
        assertEquals("Apartado Postal 565-A, C.P 62210, Cuernavaca, Morelos", affs.get(5).getAddress());
        assertEquals("Programa de Genómica Evolutiva, Centro de Ciencias Genómicas, Universidad Nacional Autónoma de México, Apartado Postal 565-A, C.P 62210, Cuernavaca, Morelos, México", affs.get(5).getRawText());

        // this case does not defined add-line element
        assertEquals("Department of Oncogenomics, Academic Medical Center, Amsterdam, 1105 AZ The Netherlands", affs.get(6).getRawText());
        assertEquals("NL", affs.get(6).getCountryCode());
        assertEquals("The Netherlands", affs.get(6).getCountryName());
        assertEquals("Department of Oncogenomics, Academic Medical Center", affs.get(6).getOrganization());
        assertEquals("Amsterdam, 1105 AZ The Netherlands", affs.get(6).getAddress());

    }
    
    @Test
    public void testComplexAffiliationsFromSpringerWithCountryCodeToBeInferred() throws Exception {
        // given
        File xmlFile = new File(XML_BASE_PATH + "/document_with_country_code_to_be_inferred.xml");
        
        // execute
        saxParser.parse(xmlFile, articleMetaXmlHandler);
        ExtractedDocumentMetadata metadata = metaBuilder.build();
        
        // assert
        
        List<Affiliation> affs = metadata.getAffiliations();
        
        assertEquals(2, affs.size());
        
        assertEquals("DE", affs.get(0).getCountryCode());
        assertEquals("Germany", affs.get(0).getCountryName());
        assertEquals("Max-Planck-Institute for Immunobiology", affs.get(0).getOrganization());
        assertEquals("Schillhof 5, 79110 Freiburg", affs.get(0).getAddress());
        assertEquals("Max-Planck-Institute for Immunobiology, Schillhof 5, 79110 Freiburg, Germany", affs.get(0).getRawText());

        //this case is to prove the country code explicitly defined in source file has precedence over the inferred one 
        assertEquals("DK_from_XML", affs.get(1).getCountryCode());
        assertEquals("Denmark", affs.get(1).getCountryName());
        assertEquals("Novozymes AS", affs.get(1).getOrganization());
        assertEquals("Bagsvaerd", affs.get(1).getAddress());
        assertEquals("Novozymes AS, Bagsvaerd, Denmark", affs.get(1).getRawText());
    }
    
}
