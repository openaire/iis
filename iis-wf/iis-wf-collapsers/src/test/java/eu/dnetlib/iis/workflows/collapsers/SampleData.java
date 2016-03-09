package eu.dnetlib.iis.workflows.collapsers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.collapsers.schemas.DocumentMetadata;
import eu.dnetlib.iis.collapsers.schemas.DocumentMetadataEnvelope;
import eu.dnetlib.iis.collapsers.schemas.DocumentTextEnvelope;
import eu.dnetlib.iis.collapsers.schemas.PublicationType;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;

/**
 *
 * @author Dominika Tkaczyk
 */
public class SampleData {

    /* parameters */
    
    public static final List<String> origins = Lists.newArrayList("origin1", "origin2");
    
    public static final List<String> significantFields = Lists.newArrayList("title", "authorIds", "abstract", "journal", "year");

    
    /* input records */

    public static final DocumentMetadata metadataRecord11 = DocumentMetadata.newBuilder()
                .setId("id-1")
                .setAbstract$("abstract 1")
                .setLanguage("en")
                .setYear(1990)
                .setKeywords(Lists.newArrayList((CharSequence)"kwd 1", "kwd 2"))
                .setDatasourceIds(Lists.newArrayList((CharSequence)"d 1", "d 2"))
                .setPublisher("publisher 1")
                .setPublicationType(PublicationType.newBuilder().setArticle(true).build())
                .build();
    
    public static final DocumentMetadataEnvelope envMetadataRecord11 = DocumentMetadataEnvelope.newBuilder()
            .setOrigin("origin1")
            .setData(metadataRecord11).build();
    
    public static final DocumentMetadata metadataRecord12 = DocumentMetadata.newBuilder()
                .setId("id-1")
                .setAbstract$("abstract 2")
                .setAuthorIds(Lists.newArrayList((CharSequence)"aut 1", "aut 2"))
                .setPublicationType(PublicationType.newBuilder().setArticle(true).build())
                .setTitle("title 2")
                .setYear(1991)
                .build();
    
    public static final DocumentMetadataEnvelope envMetadataRecord12 = DocumentMetadataEnvelope.newBuilder()
            .setOrigin("origin1")
            .setData(metadataRecord12).build();
    
    public static final DocumentMetadata metadataRecord13 = DocumentMetadata.newBuilder()
                .setId("id-1")
                .setPublicationType(PublicationType.newBuilder().setArticle(true).build())
                .build();
    
    public static final DocumentMetadataEnvelope envMetadataRecord13 = DocumentMetadataEnvelope.newBuilder()
            .setOrigin("origin1")
            .setData(metadataRecord13).build();
    
    public static final DocumentMetadata metadataRecord21 = DocumentMetadata.newBuilder()
                .setId("id-1")
                .setAbstract$("abstract 3")
                .setAuthorIds(Lists.newArrayList((CharSequence)"aut 13", "aut 23"))
                .setPublicationType(PublicationType.newBuilder().setArticle(true).build())
                .setTitle("title 3")
                .setYear(1999)
                .build();
    
    public static final DocumentMetadataEnvelope envMetadataRecord21 = DocumentMetadataEnvelope.newBuilder()
            .setOrigin("origin2")
            .setData(metadataRecord21).build();
    
    public static final DocumentMetadata metadataRecord22 = DocumentMetadata.newBuilder()
                .setId("id-1")
                .setPublicationType(PublicationType.newBuilder().setArticle(true).build())
                .build();
    
    public static final DocumentMetadataEnvelope envMetadataRecord22 = DocumentMetadataEnvelope.newBuilder()
            .setOrigin("origin2")
            .setData(metadataRecord22).build();
    
    public static final DocumentText textRecord = DocumentText.newBuilder()
                 .setId("text-1")
                 .setText("text text")
                 .build();
    
    public static final DocumentTextEnvelope envTextRecord = DocumentTextEnvelope.newBuilder()
            .setOrigin("origin1")
            .setData(textRecord).build();

    
    /* merged records */
    
    public static final DocumentMetadata mergedRecord1112 = DocumentMetadata.newBuilder()
                .setId("id-1")
                .setAbstract$("abstract 1")
                .setLanguage("en")
                .setAuthorIds(Lists.newArrayList((CharSequence)"aut 1", "aut 2"))
                .setTitle("title 2")
                .setYear(1990)
                .setKeywords(Lists.newArrayList((CharSequence)"kwd 1", "kwd 2"))
                .setDatasourceIds(Lists.newArrayList((CharSequence)"d 1", "d 2"))
                .setPublisher("publisher 1")
                .setPublicationType(PublicationType.newBuilder().setArticle(true).build())
                .build();
    
    public static final DocumentMetadata mergedRecord1211 = DocumentMetadata.newBuilder()
                 .setId("id-1")
                 .setAbstract$("abstract 2")
                 .setAuthorIds(Lists.newArrayList((CharSequence)"aut 1", "aut 2"))
                 .setTitle("title 2")
                 .setLanguage("en")
                 .setYear(1991)
                 .setKeywords(Lists.newArrayList((CharSequence)"kwd 1", "kwd 2"))
                 .setDatasourceIds(Lists.newArrayList((CharSequence)"d 1", "d 2"))
                 .setPublisher("publisher 1")
                 .setPublicationType(PublicationType.newBuilder().setArticle(true).build())
                 .build();
    
    public static final DocumentMetadata mergedRecord1121 = DocumentMetadata.newBuilder()
                .setId("id-1")
                .setAbstract$("abstract 1")
                .setLanguage("en")
                .setYear(1990)
                .setAuthorIds(Lists.newArrayList((CharSequence)"aut 13", "aut 23"))
                .setTitle("title 3")
                .setKeywords(Lists.newArrayList((CharSequence)"kwd 1", "kwd 2"))
                .setDatasourceIds(Lists.newArrayList((CharSequence)"d 1", "d 2"))
                .setPublisher("publisher 1")
                .setPublicationType(PublicationType.newBuilder().setArticle(true).build())
                .build();
    
    public static final DocumentMetadata mergedRecord2221 = DocumentMetadata.newBuilder()
                .setId("id-1")
                .setAbstract$("abstract 3")
                .setAuthorIds(Lists.newArrayList((CharSequence)"aut 13", "aut 23"))
                .setPublicationType(PublicationType.newBuilder().setArticle(true).build())
                .setTitle("title 3")
                .setYear(1999)
                .build();
    
  
    /* collapsed records */
    
    // within no merge, between no merge
    public static final DocumentMetadata recordWNoMergeBNoMerge = DocumentMetadata.newBuilder()
                .setId("id-1")
                .setAbstract$("abstract 2")
                .setAuthorIds(Lists.newArrayList((CharSequence)"aut 1", "aut 2"))
                .setPublicationType(PublicationType.newBuilder().setArticle(true).build())
                .setTitle("title 2")
                .setYear(1991)
                .build();

    // within merge, between no merge
    public static final DocumentMetadata recordWMergeBNoMerge = DocumentMetadata.newBuilder()
                .setId("id-1")
                .setAbstract$("abstract 2")
                .setLanguage("en")
                .setAuthorIds(Lists.newArrayList((CharSequence)"aut 1", "aut 2"))
                .setTitle("title 2")
                .setYear(1991)
                .setKeywords(Lists.newArrayList((CharSequence)"kwd 1", "kwd 2"))
                .setDatasourceIds(Lists.newArrayList((CharSequence)"d 1", "d 2"))
                .setPublisher("publisher 1")
                .setPublicationType(PublicationType.newBuilder().setArticle(true).build())
                .build();
    
    // within no merge, between merge
    public static final DocumentMetadata recordWNoMergeBMerge = DocumentMetadata.newBuilder()
                .setId("id-1")
                .setAbstract$("abstract 2")
                .setAuthorIds(Lists.newArrayList((CharSequence)"aut 1", "aut 2"))
                .setPublicationType(PublicationType.newBuilder().setArticle(true).build())
                .setTitle("title 2")
                .setYear(1991)
                .build();
    
    // within merge, between merge
    public static final DocumentMetadata recordWMergeBMerge = DocumentMetadata.newBuilder()
                .setId("id-1")
                .setAbstract$("abstract 2")
                .setLanguage("en")
                .setAuthorIds(Lists.newArrayList((CharSequence)"aut 1", "aut 2"))
                .setTitle("title 2")
                .setYear(1991)
                .setKeywords(Lists.newArrayList((CharSequence)"kwd 1", "kwd 2"))
                .setDatasourceIds(Lists.newArrayList((CharSequence)"d 1", "d 2"))
                .setPublisher("publisher 1")
                .setPublicationType(PublicationType.newBuilder().setArticle(true).build())
                .build();
    
    public static void assertEqualRecords(IndexedRecord expected, IndexedRecord actual) {
        assertEquals("Records are not equal: \nExpected: " + expected + "\nActual: " + actual + "\n", 
                0, GenericData.get().compare(expected, actual, expected.getSchema()));
    }
    
    public static <T extends IndexedRecord> void assertEqualRecords(List<T> expected, List<T> actual) {
        assertEquals("Records lists have different sizes: " + expected.size() + " and " + actual.size() + "\n",
                expected.size(), actual.size());
        List<T> actualCopy = Lists.newArrayList(actual);
        for (T exp : expected) {
            T found = null;
            for (T act : actualCopy) {
                if (0 == GenericData.get().compare(exp, act, exp.getSchema())) {
                    found = act;
                }
            }
            assertTrue(
                    "Expected record " + exp.toString() + " was not found among the actual records\n", 
                    found != null);
            actualCopy.remove(found);
        }
    }

}
