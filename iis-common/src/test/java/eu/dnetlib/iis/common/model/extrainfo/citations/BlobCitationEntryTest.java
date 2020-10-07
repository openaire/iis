package eu.dnetlib.iis.common.model.extrainfo.citations;

import datafu.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author mhorst
 *
 */
public class BlobCitationEntryTest {


    @Test
    public void testEquals() {
        // given
        String rawText = "some raw text";
        int position = 1;
        List<TypedId> identifiers = Lists.newArrayList(new TypedId("someValue", "someType", 0.9f));
        
        BlobCitationEntry blobCitationEntry = new BlobCitationEntry(rawText);
        blobCitationEntry.setPosition(position);
        blobCitationEntry.setIdentifiers(identifiers);
        
        // execute & assert
        assertNotNull(blobCitationEntry);
        assertNotEquals("string", blobCitationEntry);
        
        BlobCitationEntry otherBlobCitationEntry = new BlobCitationEntry("other raw text");
        otherBlobCitationEntry.setPosition(position);
        otherBlobCitationEntry.setIdentifiers(identifiers);
        assertNotEquals(blobCitationEntry, otherBlobCitationEntry);

        otherBlobCitationEntry = new BlobCitationEntry(rawText);
        otherBlobCitationEntry.setPosition(2);
        otherBlobCitationEntry.setIdentifiers(identifiers);
        assertNotEquals(blobCitationEntry, otherBlobCitationEntry);
        
        otherBlobCitationEntry = new BlobCitationEntry(rawText);
        otherBlobCitationEntry.setPosition(position);
        otherBlobCitationEntry.setIdentifiers(Lists.newArrayList(new TypedId("someOtherValue", "someType", 0.9f)));
        assertNotEquals(blobCitationEntry, otherBlobCitationEntry);
        
        otherBlobCitationEntry = new BlobCitationEntry(rawText);
        otherBlobCitationEntry.setPosition(position);
        otherBlobCitationEntry.setIdentifiers(identifiers);
        assertEquals(blobCitationEntry, otherBlobCitationEntry);
    }

    @Test
    public void testHashCode() {
        // given
        String rawText = "some raw text";
        int position = 1;
        List<TypedId> identifiers = Lists.newArrayList(new TypedId("someValue", "someType", 0.9f));
        
        BlobCitationEntry blobCitationEntry = new BlobCitationEntry(rawText);
        blobCitationEntry.setPosition(position);
        blobCitationEntry.setIdentifiers(identifiers);
        
        // execute & assert
        assertNotNull(blobCitationEntry);
        assertNotEquals("string", blobCitationEntry);
        
        BlobCitationEntry otherBlobCitationEntry = new BlobCitationEntry("other raw text");
        otherBlobCitationEntry.setPosition(position);
        otherBlobCitationEntry.setIdentifiers(identifiers);
        assertNotEquals(blobCitationEntry.hashCode(), otherBlobCitationEntry.hashCode());

        otherBlobCitationEntry = new BlobCitationEntry(rawText);
        otherBlobCitationEntry.setPosition(2);
        otherBlobCitationEntry.setIdentifiers(identifiers);
        assertNotEquals(blobCitationEntry.hashCode(), otherBlobCitationEntry.hashCode());
        
        otherBlobCitationEntry = new BlobCitationEntry(rawText);
        otherBlobCitationEntry.setPosition(position);
        otherBlobCitationEntry.setIdentifiers(Lists.newArrayList(new TypedId("someOtherValue", "someType", 0.9f)));
        assertNotEquals(blobCitationEntry.hashCode(), otherBlobCitationEntry.hashCode());
        
        otherBlobCitationEntry = new BlobCitationEntry(rawText);
        otherBlobCitationEntry.setPosition(position);
        otherBlobCitationEntry.setIdentifiers(identifiers);
        assertEquals(blobCitationEntry.hashCode(), otherBlobCitationEntry.hashCode());
    }
    
}
