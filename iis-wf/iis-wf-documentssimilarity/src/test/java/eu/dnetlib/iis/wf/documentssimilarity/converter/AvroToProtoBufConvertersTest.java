package eu.dnetlib.iis.wf.documentssimilarity.converter;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import eu.dnetlib.iis.documentssimilarity.schemas.DocumentMetadata;
import eu.dnetlib.iis.wf.documentssimilarity.producer.DocumentAvroDatastoreProducer;
import pl.edu.icm.coansys.models.DocumentProtos;

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
public class AvroToProtoBufConvertersTest {
    @Test
    public void basicTest() {
        List<DocumentMetadata> list = DocumentAvroDatastoreProducer.getDocumentMetadataList();
        DocumentProtos.DocumentWrapper converted0 =
                new DocumentMetadataAvroToProtoBufConverter().convertIntoValue(list.get(0));
        DocumentProtos.DocumentMetadata meta0 = converted0.getDocumentMetadata();
        Assert.assertEquals("1", meta0.getKey());
        Assert.assertEquals("Jan Kowalski", meta0.getBasicMetadata().getAuthor(0).getName());
        Assert.assertEquals("A new method of something", meta0.getBasicMetadata().getTitle(0).getText());
        Assert.assertArrayEquals(new String []{"method", "something", "nothing", "anything"},
                meta0.getKeywords(0).getKeywordsList().toArray(new String[meta0.getKeywords(0).getKeywordsCount()]));

    }
}
