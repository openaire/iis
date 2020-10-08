package eu.dnetlib.iis.wf.citationmatching.converter.entity_id;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
public class DocEntityIdTest {
    @Test
    public void basicTest() {
        String docId = "id12345";
        DocEntityId id = DocEntityId.parseFrom(new DocEntityId(docId).toString());
        assertEquals(docId, id.getDocumentId());
    }
}
