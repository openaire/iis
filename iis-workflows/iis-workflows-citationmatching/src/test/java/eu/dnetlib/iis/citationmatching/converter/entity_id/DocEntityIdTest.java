package eu.dnetlib.iis.citationmatching.converter.entity_id;

// TODO MiconCodeReview: Unused import
import eu.dnetlib.iis.citationmatching.converter.entity_id.DocEntityId;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
public class DocEntityIdTest {
    @Test
    public void basicTest() {
        String docId = "id12345";
        DocEntityId id = DocEntityId.parseFrom(new DocEntityId(docId).toString());
        Assert.assertEquals(docId, id.getDocumentId());
    }
}
