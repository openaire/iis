package eu.dnetlib.iis.workflows.metadataextraction;

import eu.dnetlib.iis.metadataextraction.schemas.Affiliation;
import eu.dnetlib.iis.metadataextraction.schemas.Author;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata;
import eu.dnetlib.iis.workflows.metadataextraction.NlmToDocumentWithBasicMetadataConverter;

import java.util.List;

import org.apache.avro.util.Utf8;
import org.jdom.Document;
import org.jdom.input.SAXBuilder;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * {@link NlmToDocumentWithBasicMetadataConverter} test class.
 * @author mhorst
 * @author Dominika Tkaczyk
 * 
 */
public class NlmToDocumentWithBasicMetadataConverterTest {

    private static final String testXML = "/eu/dnetlib/iis/metadataextraction/nlm-example.xml";
    
    
    @Test
	public void testConvertFull() throws Exception {
        SAXBuilder builder = new SAXBuilder();
        Document document = (Document) builder.build(ClassLoader.class.getResourceAsStream(testXML));
        assertNotNull(document);
        
        String id = "predefinedId";

        ExtractedDocumentMetadata result = 
                NlmToDocumentWithBasicMetadataConverter.convertFull(id, document);
        
		assertNotNull(result);
        assertEquals(id, result.getId());
		
        testAuthorsAndAffiliations(result.getAuthors(), result.getAffiliations());
        
        testReferences(result.getReferences());
        
        assertEquals("Hindawi Publishing Corporation", result.getPublisher());
        
        assertEquals("International Journal of Digital Multimedia Broadcasting", result.getJournal());
        
		assertEquals("Video Quality Prediction Models Based on Video Content Dynamics for H.264 Video over UMTS Networks", result.getTitle());

        assertEquals(2, result.getExternalIdentifiers().size());
		assertEquals(new Utf8("10.1155/2010/608138"), result.getExternalIdentifiers().get(new Utf8("doi")));
		assertEquals(new Utf8("608138"), result.getExternalIdentifiers().get(new Utf8("hindawi-id")));
		
        assertNotNull(result.getAbstract$());
		assertFalse(result.getAbstract$().toString().isEmpty());
		
        assertEquals(2, result.getKeywords().size());
        assertTrue(result.getKeywords().contains("keyword 1"));
        assertTrue(result.getKeywords().contains("keyword 2"));
        
        assertEquals(2010, (int)result.getYear());
        
        assertEquals("2010", result.getVolume());
        assertEquals("issue-5", result.getIssue());
        
        assertNotNull(result.getPages());
        assertEquals("50", result.getPages().getStart());
        assertEquals("60", result.getPages().getEnd());
    }
    
    private void testAuthorsAndAffiliations(List<Author> authors, List<Affiliation> affiliations) {
        assertNotNull(authors);
        assertEquals(3, authors.size());
        
        assertEquals("Lingfen Sun", authors.get(0).getAuthorFullName());
        assertEquals("Fidel Liberal", authors.get(1).getAuthorFullName());
        assertEquals("Harilaos Koumaras", authors.get(2).getAuthorFullName());
        
        assertNotNull(affiliations);
        assertEquals(3, affiliations.size());
 
        assertNotNull(authors.get(0).getAffiliationPositions());
        assertEquals(2, authors.get(0).getAffiliationPositions().size());

        assertEquals("Centre for Signal Processing and Multimedia Communication, School of Computing, University of Plymouth",
                     affiliations.get(authors.get(0).getAffiliationPositions().get(0)).getOrganization());
        
        assertNotNull(authors.get(1).getAffiliationPositions());
        assertEquals(1, authors.get(1).getAffiliationPositions().size());
        assertEquals("Department of Electronics and Telecommunications, University of the Basque Country (UPV/EHU), 48013 Bilbao, Spain",
                     affiliations.get(authors.get(1).getAffiliationPositions().get(0)).getRawText());
        assertEquals("Spain",
                     affiliations.get(authors.get(1).getAffiliationPositions().get(0)).getCountryName());
        assertEquals("ES",
                     affiliations.get(authors.get(1).getAffiliationPositions().get(0)).getCountryCode());
        assertEquals("48013 Bilbao",
                     affiliations.get(authors.get(1).getAffiliationPositions().get(0)).getAddress());
        assertEquals("Department of Electronics and Telecommunications, University of the Basque Country (UPV/EHU)",
                     affiliations.get(authors.get(1).getAffiliationPositions().get(0)).getOrganization());
        
        assertNotNull(authors.get(2).getAffiliationPositions());
        assertEquals(1, authors.get(2).getAffiliationPositions().size());
        assertEquals("Institute of Informatics and Telecommunications, NCSR Demokritos, 15310 Athens, Greece",
                     affiliations.get(authors.get(2).getAffiliationPositions().get(0)).getRawText());
        assertEquals("Greece",
                     affiliations.get(authors.get(2).getAffiliationPositions().get(0)).getCountryName());
        assertEquals("GR",
                     affiliations.get(authors.get(2).getAffiliationPositions().get(0)).getCountryCode());
        assertEquals("15310 Athens",
                     affiliations.get(authors.get(2).getAffiliationPositions().get(0)).getAddress());
        assertEquals("Institute of Informatics and Telecommunications, NCSR Demokritos",
                     affiliations.get(authors.get(2).getAffiliationPositions().get(0)).getOrganization());   
    }

    private void testReferences(List<ReferenceMetadata> references) {
        assertNotNull(references);
        assertEquals(2, references.size());
        
        ReferenceMetadata ref1 = references.get(0);
        assertEquals(1, (int)ref1.getPosition());
        assertEquals("[1] E. Braunwald , “ Shattuck lecture: cardiovascular medicine at the turn of the millennium: triumphs , concerns, and opportunities,” New England Journal of Medicine , vol. 337 , no. 19 , pp. 1360 – 1369 , 1997 .", 
                ref1.getText());
        assertNotNull(ref1.getBasicMetadata());
        assertNotNull(ref1.getBasicMetadata().getAuthors());
        assertEquals(1, ref1.getBasicMetadata().getAuthors().size());
        assertTrue(ref1.getBasicMetadata().getAuthors().contains("Braunwald, E."));
        assertEquals("19", ref1.getBasicMetadata().getIssue());
        assertEquals("New England Journal of Medicine", ref1.getBasicMetadata().getSource());
        assertEquals("Shattuck lecture: cardiovascular medicine at the turn of the millennium: triumphs", 
                ref1.getBasicMetadata().getTitle());
        assertEquals("337", ref1.getBasicMetadata().getVolume());
        assertEquals("1997", ref1.getBasicMetadata().getYear());
        assertNotNull(ref1.getBasicMetadata().getPages());
        assertEquals("1360", ref1.getBasicMetadata().getPages().getStart());
        assertEquals("1369", ref1.getBasicMetadata().getPages().getEnd());
        
        ReferenceMetadata ref2 = references.get(1);
        assertEquals(2, (int)ref2.getPosition());
        assertEquals("[2] R. L. Campbell , R. Banner , J. Konick-McMahan , and M. D. Naylor , “ Discharge planning and home follow-up of the elderly patient with heart failure ,” The Nursing Clinics of North America , vol. 33 , no. 3 , pp. 497 , 1998 .", 
                ref2.getText());
        assertNotNull(ref2.getBasicMetadata());
        assertNotNull(ref2.getBasicMetadata().getAuthors());
        assertEquals(4, ref2.getBasicMetadata().getAuthors().size());
        assertTrue(ref2.getBasicMetadata().getAuthors().contains("Campbell, R. L."));
        assertTrue(ref2.getBasicMetadata().getAuthors().contains("Banner, R."));
        assertTrue(ref2.getBasicMetadata().getAuthors().contains("Konick-McMahan, J."));
        assertTrue(ref2.getBasicMetadata().getAuthors().contains("Naylor, M. D."));
        assertEquals("3", ref2.getBasicMetadata().getIssue());
        assertEquals("The Nursing Clinics of North America", ref2.getBasicMetadata().getSource());
        assertEquals("Discharge planning and home follow-up of the elderly patient with heart failure", 
                ref2.getBasicMetadata().getTitle());
        assertEquals("33", ref2.getBasicMetadata().getVolume());
        assertEquals("1998", ref2.getBasicMetadata().getYear());
        assertNotNull(ref2.getBasicMetadata().getPages());
        assertEquals("497", ref2.getBasicMetadata().getPages().getStart());
        assertEquals("497", ref2.getBasicMetadata().getPages().getEnd());
    }
        
}
