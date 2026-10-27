package eu.dnetlib.iis.common.model.extrainfo.converter;

import eu.dnetlib.iis.common.model.extrainfo.citations.BlobCitationEntry;
import eu.dnetlib.iis.common.model.extrainfo.citations.BlobCitationBasicMetadata;
import eu.dnetlib.iis.common.model.extrainfo.citations.TypedId;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author mhorst
 *
 */
public class CitationsExtraInfoSerDeTest {

    CitationsExtraInfoSerDe serDe = new CitationsExtraInfoSerDe();
    
    @Test
    public void testSerializeNull() throws Exception {
        String result = serDe.serialize(null);
        assertEquals("<null/>", result);
    }
    
    @Test
    public void testDeserializeNull() {
        assertThrows(NullPointerException.class, () -> serDe.deserialize(null));
    }
    
    @Test
    public void testGetXStream() throws Exception {
        assertNotNull(serDe.getXstream());
    }
    
    @Test
    public void testSerializeAndDeserialize() throws Exception {
        // given
        int position = 1;
        String rawText = "citation raw text";
        
        String idValue = "val";
        String idType = "type";
        float trustLevel = 0.9f;
        
        List<TypedId> identifiers = new ArrayList<>();
        identifiers.add(new TypedId(idValue, idType, trustLevel));
        
        BlobCitationEntry entry = new BlobCitationEntry();
        entry.setPosition(position);
        entry.setRawText(rawText);
        entry.setIdentifiers(identifiers);
        
        SortedSet<BlobCitationEntry> entrySet = new TreeSet<>();
        entrySet.add(entry);
        
        // execute
        String resultStr = serDe.serialize(entrySet);
        SortedSet<BlobCitationEntry> resultSet = serDe.deserialize(resultStr);

        // assert
        assertFalse(resultStr.contains("basicMetadata"));
        assertNotNull(resultSet);
        assertEquals(1, resultSet.size());
        BlobCitationEntry resultEntry = resultSet.first();
        assertNotNull(resultEntry);
        assertEquals(position, resultEntry.getPosition());
        assertEquals(rawText, resultEntry.getRawText());
        assertNotNull(resultEntry.getIdentifiers());
        assertEquals(1, resultEntry.getIdentifiers().size());
        TypedId resultId = resultEntry.getIdentifiers().get(0);
        assertEquals(idValue, resultId.getValue());
        assertEquals(idType, resultId.getType());
        assertEquals(trustLevel, resultId.getTrustLevel(), 0.001f);
    }

    @Test
    public void testSerializeBasicMetadata() {
        BlobCitationBasicMetadata basicMetadata = new BlobCitationBasicMetadata();
        basicMetadata.setTitle("Citation title");
        basicMetadata.setAuthors(Arrays.asList("A. Author", "B. Author"));
        basicMetadata.setPages(new BlobCitationBasicMetadata.Pages("10", "20"));

        BlobCitationEntry entry = new BlobCitationEntry();
        entry.setPosition(1);
        entry.setBasicMetadata(basicMetadata);

        SortedSet<BlobCitationEntry> entries = new TreeSet<>();
        entries.add(entry);

        String result = serDe.serialize(entries);

        assertTrue(result.contains("<basicMetadata>"));
        assertTrue(result.contains("<title>Citation title</title>"));
        assertTrue(result.contains("<author>A. Author</author>"));
        assertTrue(result.contains("<author>B. Author</author>"));
        assertTrue(result.contains("<pages start=\"10\" end=\"20\"/>"));
        assertFalse(result.contains("<authors>"));
        assertFalse(result.contains("<string>"));
        assertFalse(result.contains("class=\"null\""));
        assertFalse(result.contains("<externalIds"));
        assertFalse(result.contains("<source"));
    }

}
