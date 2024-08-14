package eu.dnetlib.iis.wf.ingest.pmc.metadata;

import static eu.dnetlib.iis.wf.ingest.pmc.metadata.AssertExtractedDocumentMetadata.assertAffiliation;
import static eu.dnetlib.iis.wf.ingest.pmc.metadata.AssertExtractedDocumentMetadata.assertAuthor;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
    
}
