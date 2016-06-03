package eu.dnetlib.iis.wf.importer.infospace.converter;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import eu.dnetlib.data.proto.FieldTypeProtos.StructuredProperty;
import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.data.proto.ResultProtos.Result;
import eu.dnetlib.data.proto.ResultProtos.Result.Metadata;
import eu.dnetlib.data.proto.TypeProtos.Type;
import eu.dnetlib.iis.common.hbase.HBaseConstants;
import eu.dnetlib.iis.importer.schemas.DocumentMetadata;
import eu.dnetlib.iis.wf.importer.infospace.QualifiedOafJsonRecord;
import eu.dnetlib.iis.wf.importer.infospace.approver.FieldApprover;
import eu.dnetlib.iis.wf.importer.infospace.approver.ResultApprover;

/**
 * {@link DocumentMetadataConverter} test class.
 */
public class DocumentMetadataConverterTest {

    private static final String ID = "document id";
    private static final String TITLE = "Document Title";
    private static final String OTHER_TITLE = "Other "+TITLE;
    private static final String ABSTRACT = "Document abstract";
    private static final String LANGUAGE = "polish";
    private static final ImmutableList<String> KEYWORDS = ImmutableList.of("keyword 1", "keyword 2");
    private static final ImmutableMap<String, String> EXT_IDENTIFIERS = ImmutableMap.of("k1", "v1", "k2", "v2");
    private static final String JOURNAL = "Journal Title";
    private static final Integer YEAR = 2000;
    private static final String PUBLISHER = "Publisher Name";
    private static final ImmutableList<String> AUTHOR_IDS = ImmutableList.of("id 1", "id #2", "third id");
    private static final ImmutableList<String> DATASOURCE_IDS = ImmutableList.of("source id 1", "id #2");

    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @InjectMocks
    private DocumentMetadataConverter converter;

    @Mock
    private ResultApprover resultApprover;

    @Mock
    private FieldApprover fieldApprover;

    @Before
    public void setUp() {
        when(fieldApprover.approve(any())).thenReturn(true);
        when(resultApprover.approve(any())).thenReturn(true);
    }

    // ------------------------ TESTS --------------------------

    @Test(expected=NullPointerException.class)
    public void convert_null_oafEntity() throws IOException {
        // execute
        converter.convert(null, null);
    }

    @Test
    public void convert_using_main_title() throws IOException {
        // given
        OafEntity.Builder builder = minimalEntityBuilder(ID, HBaseConstants.SEMANTIC_CLASS_INSTANCE_TYPE_ARTICLE);

        addTitle(builder, OTHER_TITLE);
        addTitle(builder, TITLE).getQualifierBuilder().setClassid(HBaseConstants.SEMANTIC_CLASS_MAIN_TITLE);

        OafEntity oafEntity = builder.build();

        // execute
        DocumentMetadata metadata = converter.convert(oafEntity, emptyMap());

        // assert
        assertEquals(TITLE, metadata.getTitle());
    }

    @Test
    public void convert_using_first_title() throws IOException {
        // given
        OafEntity.Builder builder = minimalEntityBuilder(ID, HBaseConstants.SEMANTIC_CLASS_INSTANCE_TYPE_ARTICLE);

        addTitle(builder, OTHER_TITLE);
        addTitle(builder, TITLE);

        OafEntity oafEntity = builder.build();

        // execute
        DocumentMetadata metadata = converter.convert(oafEntity, emptyMap());

        // assert
        assertEquals(OTHER_TITLE, metadata.getTitle());
    }

    @Test
    public void convert_skip_null_abstract() throws IOException {
        // given
        OafEntity.Builder builder = minimalEntityBuilder(ID, HBaseConstants.SEMANTIC_CLASS_INSTANCE_TYPE_ARTICLE);

        addDescription(builder, "null");
        addDescription(builder, ABSTRACT);

        OafEntity oafEntity = builder.build();

        // execute
        DocumentMetadata metadata = converter.convert(oafEntity, emptyMap());

        // assert
        assertEquals(ABSTRACT, metadata.getAbstract$());
    }

    @Test
    public void convert_with_undefined_language() throws IOException {
        // given
        OafEntity.Builder builder = minimalEntityBuilder(ID, HBaseConstants.SEMANTIC_CLASS_INSTANCE_TYPE_ARTICLE);

        setLanguage(builder, "und");

        OafEntity oafEntity = builder.build();

        // execute
        DocumentMetadata metadata = converter.convert(oafEntity, emptyMap());

        // assert
        assertEquals(null, metadata.getLanguage());
    }


    @Test
    public void convert_not_approved() throws IOException {
        // given
        OafEntity oafEntity = documentEntity();
        ImmutableMap<String, List<QualifiedOafJsonRecord>> relations = authorRelations();

        when(fieldApprover.approve(any())).thenReturn(false);
        when(resultApprover.approve(any())).thenReturn(false);

        // execute
        DocumentMetadata metadata = converter.convert(oafEntity, relations);

        // assert
        assertEquals(ID, metadata.getId());
        assertEquals(null, metadata.getTitle());
        assertEquals(null, metadata.getAbstract$());
        assertEquals(LANGUAGE, metadata.getLanguage());
        assertEquals(null, metadata.getKeywords());
        assertEquals(null, metadata.getExternalIdentifiers());
        assertEquals(null, metadata.getJournal());
        assertEquals(null, metadata.getYear());
        assertEquals(null, metadata.getPublisher());
        assertTrue(metadata.getPublicationType().getArticle());
        assertTrue(metadata.getPublicationType().getDataset());
        assertEquals(null, metadata.getAuthorIds());
        assertEquals(DATASOURCE_IDS, metadata.getDatasourceIds());
    }

    @Test
    public void convert() throws IOException {
        // given
        OafEntity oafEntity = documentEntity();
        ImmutableMap<String, List<QualifiedOafJsonRecord>> relations = authorRelations();

        // execute
        DocumentMetadata metadata = converter.convert(oafEntity, relations);

        // assert
        assertEquals(ID, metadata.getId());
        assertEquals(TITLE, metadata.getTitle());
        assertEquals(ABSTRACT, metadata.getAbstract$());
        assertEquals(LANGUAGE, metadata.getLanguage());
        assertEquals(KEYWORDS, metadata.getKeywords());
        assertEquals(EXT_IDENTIFIERS, metadata.getExternalIdentifiers());
        assertEquals(JOURNAL, metadata.getJournal());
        assertEquals(YEAR, metadata.getYear());
        assertEquals(PUBLISHER, metadata.getPublisher());
        assertTrue(metadata.getPublicationType().getArticle());
        assertTrue(metadata.getPublicationType().getDataset());
        assertEquals(AUTHOR_IDS, metadata.getAuthorIds());
        assertEquals(DATASOURCE_IDS, metadata.getDatasourceIds());
    }

    // ------------------------ PRIVATE --------------------------

    private static OafEntity.Builder emptyEntityBuilder(String id) {
        // note that the type does not matter for the converter
        return OafEntity.newBuilder().setType(Type.result).setId(id);
    }

    private static OafEntity.Builder minimalEntityBuilder(String id, String... types) {
        OafEntity.Builder builder = emptyEntityBuilder(id);
        addPublicationTypes(builder.getResultBuilder(), types);
        return builder;
    }

    private static OafEntity documentEntity() {
        OafEntity.Builder oafBuilder = minimalEntityBuilder(ID,
                HBaseConstants.SEMANTIC_CLASS_INSTANCE_TYPE_ARTICLE,
                HBaseConstants.SEMANTIC_CLASS_INSTANCE_TYPE_DATASET);

        Metadata.Builder mdBuilder = oafBuilder.getResultBuilder().getMetadataBuilder();

        addTitle(oafBuilder, TITLE);
        addDescription(oafBuilder, ABSTRACT);
        setLanguage(oafBuilder, LANGUAGE);

        for (String keyword : KEYWORDS) {
            mdBuilder.addSubjectBuilder().setValue(keyword);
        }

        for (Entry<String, String> entry : EXT_IDENTIFIERS.entrySet()) {
            oafBuilder.addPidBuilder().setValue(entry.getValue()).getQualifierBuilder().setClassid(entry.getKey());
        }

        mdBuilder.getJournalBuilder().setName(JOURNAL);
        mdBuilder.getDateofacceptanceBuilder().setValue(String.format("%s-02-29", YEAR));
        mdBuilder.getPublisherBuilder().setValue(PUBLISHER);


        for (String id : DATASOURCE_IDS) {
            oafBuilder.addCollectedfromBuilder().setKey(id);
        }

        return oafBuilder.build();
    }

    private static void addPublicationTypes(Result.Builder resBuilder, String... types) {
        for (String type : types) {
            resBuilder.addInstanceBuilder().getInstancetypeBuilder()
                .setClassid(type);
        }
    }

    private static StructuredProperty.Builder addTitle(OafEntity.Builder builder, String value) {
        return builder.getResultBuilder().getMetadataBuilder().addTitleBuilder().setValue(value);
    }

    private static void addDescription(OafEntity.Builder builder, String value) {
        builder.getResultBuilder().getMetadataBuilder().addDescriptionBuilder().setValue(value);
    }

    private static void setLanguage(OafEntity.Builder builder, String value) {
        builder.getResultBuilder().getMetadataBuilder().getLanguageBuilder().setClassid(value);
    }

    private static ImmutableMap<String, List<QualifiedOafJsonRecord>> authorRelations() {
        String jsonTemplate = "{\"kind\":\"relation\",\"rel\":{\"relType\":\"personResult\",\"subRelType\":\"authorship\",\"relClass\":\"hasAuthor\",\"source\":\"srcId\",\"target\":\"%s\",\"child\":false,\"personResult\":{\"authorship\":{\"ranking\":\"%s\",\"relMetadata\":{}}}}}";

        ArrayList<QualifiedOafJsonRecord> jsons = new ArrayList<>();

        for (int i = 0; i < AUTHOR_IDS.size(); i++) {
            jsons.add(new QualifiedOafJsonRecord("qualifier", String.format(jsonTemplate, AUTHOR_IDS.get(i), i)));
        }

        return ImmutableMap.of("personResult_authorship_hasAuthor", jsons);
    }
}
