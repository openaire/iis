package eu.dnetlib.iis.workflows.citationmatching.converter.entity_id;

// TODO MiconCodeReview: Unused import
import eu.dnetlib.iis.workflows.citationmatching.converter.entity_id.CitEntityId;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
public class CitEntityIdTest {
    @Test
    public void basicTest() {
        String src = "id12345";
        int pos = 44;
        CitEntityId id = CitEntityId.parseFrom(new CitEntityId(src, pos).toString());
        Assert.assertEquals(src, id.getSourceDocumentId());
        Assert.assertEquals(pos, id.getPosition());
    }
}
