package eu.dnetlib.iis.common.model.extrainfo.citations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import datafu.com.google.common.collect.Lists;

/**
 * @author mhorst
 *
 */
public class BlobCitationEntryTest {


    @Test
    public void testEquals() throws Exception {
        // given
        String rawText = "some raw text";
        int position = 1;
        List<TypedId> identifiers = Lists.newArrayList(new TypedId("someValue", "someType", 0.9f));
        
        BlobCitationEntry blobCitationEntry = new BlobCitationEntry(rawText);
        blobCitationEntry.setPosition(position);
        blobCitationEntry.setIdentifiers(identifiers);
        
        // execute & assert
        assertFalse(blobCitationEntry.equals(null));
        assertFalse(blobCitationEntry.equals("string"));
        
        BlobCitationEntry otherBlobCitationEntry = new BlobCitationEntry("other raw text");
        otherBlobCitationEntry.setPosition(position);
        otherBlobCitationEntry.setIdentifiers(identifiers);
        assertFalse(blobCitationEntry.equals(otherBlobCitationEntry));

        otherBlobCitationEntry = new BlobCitationEntry(rawText);
        otherBlobCitationEntry.setPosition(2);
        otherBlobCitationEntry.setIdentifiers(identifiers);
        assertFalse(blobCitationEntry.equals(otherBlobCitationEntry));
        
        otherBlobCitationEntry = new BlobCitationEntry(rawText);
        otherBlobCitationEntry.setPosition(position);
        otherBlobCitationEntry.setIdentifiers(Lists.newArrayList(new TypedId("someOtherValue", "someType", 0.9f)));
        assertFalse(blobCitationEntry.equals(otherBlobCitationEntry));
        
        otherBlobCitationEntry = new BlobCitationEntry(rawText);
        otherBlobCitationEntry.setPosition(position);
        otherBlobCitationEntry.setIdentifiers(identifiers);
        assertTrue(blobCitationEntry.equals(otherBlobCitationEntry));
    }

    @Test
    public void testHashCode() throws Exception {
        // given
        String rawText = "some raw text";
        int position = 1;
        List<TypedId> identifiers = Lists.newArrayList(new TypedId("someValue", "someType", 0.9f));
        
        BlobCitationEntry blobCitationEntry = new BlobCitationEntry(rawText);
        blobCitationEntry.setPosition(position);
        blobCitationEntry.setIdentifiers(identifiers);
        
        // execute & assert
        assertFalse(blobCitationEntry.equals(null));
        assertFalse(blobCitationEntry.equals("string"));
        
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
