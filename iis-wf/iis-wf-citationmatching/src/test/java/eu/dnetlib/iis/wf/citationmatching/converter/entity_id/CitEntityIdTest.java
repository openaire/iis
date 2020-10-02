package eu.dnetlib.iis.wf.citationmatching.converter.entity_id;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
public class CitEntityIdTest {
    @Test
    public void basicTest() {
        String src = "id12345";
        int pos = 44;
        CitEntityId id = CitEntityId.parseFrom(new CitEntityId(src, pos).toString());
        assertEquals(src, id.getSourceDocumentId());
        assertEquals(pos, id.getPosition());
    }
}
