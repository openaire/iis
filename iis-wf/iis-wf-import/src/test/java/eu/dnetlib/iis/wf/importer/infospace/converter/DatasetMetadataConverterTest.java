package eu.dnetlib.iis.wf.importer.infospace.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.google.common.collect.ImmutableMap;

import eu.dnetlib.data.proto.FieldTypeProtos.Author;
import eu.dnetlib.data.proto.FieldTypeProtos.Qualifier;
import eu.dnetlib.data.proto.FieldTypeProtos.StructuredProperty;
import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.data.proto.ResultProtos.Result;
import eu.dnetlib.data.proto.ResultProtos.Result.Metadata;
import eu.dnetlib.data.proto.TypeProtos.Type;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.importer.schemas.DataSetReference;
import eu.dnetlib.iis.wf.importer.infospace.approver.FieldApprover;

/**
 * {@link DatasetMetadataConverter} test class.
 */
public class DatasetMetadataConverterTest {

    private static final String ID = "dataset id";
    
    private static final String FULL_NAME = "the full name";
    private static final String TITLE = "Dataset First Title";
    private static final String OTHER_TITLE = "Other dataset title";
    private static final String ABSTRACT = "Dataset abstract";
    private static final String PUBLISHER = "Publisher Name";    
    private static final String YEAR = "2000";

    private static final String FORMAT = "some format";   
    
    private static final String RESULT_TYPE_CLASSID = "result_type";
    private static final String RESOURCE_TYPE_CLASSID = "resource_type";

    private static final String EXT_ID_TYPE_DOI_UPPERCASED = "DOI";
    
    private static final ImmutableMap<String, String> EXT_IDENTIFIERS = ImmutableMap.of(EXT_ID_TYPE_DOI_UPPERCASED, "doi-id", "other", "2");
    
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @InjectMocks
    private DatasetMetadataConverter converter;

    @Mock
    private FieldApprover fieldApprover;

    @Before
    public void setUp() {
        when(fieldApprover.approve(any())).thenReturn(true);
    }

    // ------------------------ TESTS --------------------------

    @Test(expected=NullPointerException.class)
    public void convert_null_oafEntity() throws IOException {
        // execute
        converter.convert(null);
    }

    @Test
    public void convert_using_main_title() throws IOException {
        // given
        OafEntity.Builder builder = minimalEntityBuilder(ID, InfoSpaceConstants.SEMANTIC_CLASS_INSTANCE_TYPE_DATASET);

        addTitle(builder, OTHER_TITLE);
        addTitle(builder, TITLE).getQualifierBuilder().setClassid(InfoSpaceConstants.SEMANTIC_CLASS_MAIN_TITLE);

        OafEntity oafEntity = builder.build();

        // execute
        DataSetReference metadata = converter.convert(oafEntity);

        // assert
        assertNotNull(metadata);
        assertEquals(2, metadata.getTitles().size());
        assertEquals(OTHER_TITLE, metadata.getTitles().get(0));
        assertEquals(TITLE, metadata.getTitles().get(1));
    }

    @Test
    public void convert_skip_null_abstract() throws IOException {
        // given
        OafEntity.Builder builder = minimalEntityBuilder(ID, InfoSpaceConstants.SEMANTIC_CLASS_INSTANCE_TYPE_DATASET);

        addDescription(builder, "null");
        addDescription(builder, ABSTRACT);

        OafEntity oafEntity = builder.build();

        // execute
        DataSetReference metadata = converter.convert(oafEntity);

        // assert
        assertNotNull(metadata);
        assertEquals(ABSTRACT, metadata.getDescription());
    }

    @Test
    public void convert_missing_doi() throws IOException {
        // given
        ImmutableMap<String, String> extIds = ImmutableMap.of("other", "2");
        OafEntity oafEntity = documentEntity(extIds);

        // execute
        DataSetReference metadata = converter.convert(oafEntity);

        // assert
        assertNotNull(metadata);
        assertEquals(ID, metadata.getId());
        
        assertEquals("other", metadata.getReferenceType());
        assertEquals("2", metadata.getIdForGivenType());
        
        assertEquals(1, metadata.getCreatorNames().size());
        assertEquals(FULL_NAME, metadata.getCreatorNames().get(0));
        
        assertEquals(2, metadata.getTitles().size());
        assertEquals(TITLE, metadata.getTitles().get(0));
        assertEquals(OTHER_TITLE, metadata.getTitles().get(1));
        
        assertEquals(ABSTRACT, metadata.getDescription());
        assertEquals(PUBLISHER, metadata.getPublisher());
        assertEquals(YEAR, metadata.getPublicationYear());
        
        assertEquals(1, metadata.getFormats().size());
        assertEquals(FORMAT, metadata.getFormats().iterator().next());
        
        assertEquals(RESOURCE_TYPE_CLASSID, metadata.getResourceTypeValue());
        
        assertEquals(1, metadata.getAlternateIdentifiers().size());
        assertEquals(extIds, metadata.getAlternateIdentifiers());    
    }
    
    @Test
    public void convert_not_approved() throws IOException {
        // given
        OafEntity oafEntity = documentEntity(EXT_IDENTIFIERS);

        when(fieldApprover.approve(any())).thenReturn(false);

        // execute
        DataSetReference metadata = converter.convert(oafEntity);

        // assert
        assertNotNull(metadata);
        assertEquals(ID, metadata.getId());
        
        assertEquals("unspecified", metadata.getReferenceType());
        assertEquals("", metadata.getIdForGivenType());
        
        assertEquals(1, metadata.getCreatorNames().size());
        assertEquals(FULL_NAME, metadata.getCreatorNames().get(0));
        
        assertEquals(0, metadata.getTitles().size());
        
        assertNull(metadata.getDescription());
        assertNull(metadata.getPublisher());
        assertNull(metadata.getPublicationYear());
        
        assertEquals(0, metadata.getFormats().size());
        
        assertEquals("resource_type", metadata.getResourceTypeValue());
        
        assertNull(metadata.getAlternateIdentifiers());
    }

    @Test
    public void convert() throws IOException {
        // given
        OafEntity oafEntity = documentEntity(EXT_IDENTIFIERS);

        // execute
        DataSetReference metadata = converter.convert(oafEntity);

        // assert
        assertNotNull(metadata);
        assertEquals(ID, metadata.getId());
        
        assertEquals(EXT_ID_TYPE_DOI_UPPERCASED.toLowerCase(), metadata.getReferenceType());
        assertEquals(EXT_IDENTIFIERS.get(EXT_ID_TYPE_DOI_UPPERCASED), metadata.getIdForGivenType());
        
        assertEquals(1, metadata.getCreatorNames().size());
        assertEquals(FULL_NAME, metadata.getCreatorNames().get(0));
        
        assertEquals(2, metadata.getTitles().size());
        assertEquals(TITLE, metadata.getTitles().get(0));
        assertEquals(OTHER_TITLE, metadata.getTitles().get(1));
        
        assertEquals(ABSTRACT, metadata.getDescription());
        assertEquals(PUBLISHER, metadata.getPublisher());
        assertEquals(YEAR, metadata.getPublicationYear());
        
        assertEquals(1, metadata.getFormats().size());
        assertEquals(FORMAT, metadata.getFormats().iterator().next());
        
        assertEquals(RESOURCE_TYPE_CLASSID, metadata.getResourceTypeValue());
        
        assertEquals(2, metadata.getAlternateIdentifiers().size());
        assertEquals(EXT_IDENTIFIERS, metadata.getAlternateIdentifiers());
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

    private static OafEntity documentEntity(ImmutableMap<String, String> extIdentifiers) {
        OafEntity.Builder oafBuilder = minimalEntityBuilder(ID,
                InfoSpaceConstants.SEMANTIC_CLASS_INSTANCE_TYPE_ARTICLE,
                InfoSpaceConstants.SEMANTIC_CLASS_INSTANCE_TYPE_DATASET);

        Metadata.Builder mdBuilder = oafBuilder.getResultBuilder().getMetadataBuilder();

        addTitle(oafBuilder, TITLE);
        addTitle(oafBuilder, OTHER_TITLE);
        
        addDescription(oafBuilder, ABSTRACT);

        for (Entry<String, String> entry : extIdentifiers.entrySet()) {
            oafBuilder.addPidBuilder().setValue(entry.getValue()).getQualifierBuilder().setClassid(entry.getKey());
        }

        mdBuilder.getDateofacceptanceBuilder().setValue(String.format("%s-02-29", YEAR));
        mdBuilder.getPublisherBuilder().setValue(PUBLISHER);
        
        mdBuilder.addFormatBuilder().setValue(FORMAT);

        mdBuilder.setResulttype(buildQualifierWithClassId(RESULT_TYPE_CLASSID));
        mdBuilder.setResourcetype(buildQualifierWithClassId(RESOURCE_TYPE_CLASSID));
        
        Author.Builder authorBuilder = Author.newBuilder();
        authorBuilder.setFullname(FULL_NAME);
        authorBuilder.setRank(0);
        oafBuilder.getResultBuilder().getMetadataBuilder().addAuthor(authorBuilder);
        
        return oafBuilder.build();
    }
    
    private static Qualifier buildQualifierWithClassId(String classId) {
        return Qualifier.newBuilder().setClassid(classId).build();
    }

    private static void addPublicationTypes(Result.Builder resBuilder, String... types) {
        Arrays.stream(types).forEach(type -> resBuilder.addInstanceBuilder().getInstancetypeBuilder().setClassid(type));
    }

    private static StructuredProperty.Builder addTitle(OafEntity.Builder builder, String value) {
        return builder.getResultBuilder().getMetadataBuilder().addTitleBuilder().setValue(value);
    }

    private static void addDescription(OafEntity.Builder builder, String value) {
        builder.getResultBuilder().getMetadataBuilder().addDescriptionBuilder().setValue(value);
    }

}
