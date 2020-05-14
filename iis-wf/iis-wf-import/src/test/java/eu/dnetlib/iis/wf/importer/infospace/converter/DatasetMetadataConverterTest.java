package eu.dnetlib.iis.wf.importer.infospace.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.google.common.collect.ImmutableMap;

import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.Instance;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
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
    public void convert_null_oafEntity() {
        // execute
        converter.convert(null);
    }

    @Test
    public void convert_using_main_title() {
        // given
        Result sourceDataset = minimalEntityBuilder(ID, InfoSpaceConstants.SEMANTIC_CLASS_INSTANCE_TYPE_DATASET);

        addTitle(sourceDataset, OTHER_TITLE, null);
        addTitle(sourceDataset, TITLE, InfoSpaceConstants.SEMANTIC_CLASS_MAIN_TITLE);


        // execute
        DataSetReference metadata = converter.convert(sourceDataset);

        // assert
        assertNotNull(metadata);
        assertEquals(2, metadata.getTitles().size());
        assertEquals(OTHER_TITLE, metadata.getTitles().get(0));
        assertEquals(TITLE, metadata.getTitles().get(1));
    }

    @Test
    public void convert_skip_null_abstract() {
        // given
        Result sourceDataset = minimalEntityBuilder(ID, InfoSpaceConstants.SEMANTIC_CLASS_INSTANCE_TYPE_DATASET);

        addDescription(sourceDataset, "null");
        addDescription(sourceDataset, ABSTRACT);

        // execute
        DataSetReference metadata = converter.convert(sourceDataset);

        // assert
        assertNotNull(metadata);
        assertEquals(ABSTRACT, metadata.getDescription());
    }

    @Test
    public void convert_missing_doi() {
        // given
        ImmutableMap<String, String> extIds = ImmutableMap.of("other", "2");
        Result sourceDataset = documentEntity(extIds);

        // execute
        DataSetReference metadata = converter.convert(sourceDataset);

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
    public void convert_null_date_of_acceptance() {
     // given
        Result sourceDataset = documentEntity(EXT_IDENTIFIERS);
        sourceDataset.getDateofacceptance().setValue(null);
        
        // execute
        DataSetReference metadata = converter.convert(sourceDataset);

        // assert
        assertNotNull(metadata);
        assertEquals(ID, metadata.getId());
        
        assertNull(metadata.getPublicationYear());
    }
    
    @Test
    public void convert_not_approved() {
        // given
        Result sourceDataset = documentEntity(EXT_IDENTIFIERS);

        when(fieldApprover.approve(any())).thenReturn(false);

        // execute
        DataSetReference metadata = converter.convert(sourceDataset);

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
    public void convert() {
        // given
        Result sourceDataset = documentEntity(EXT_IDENTIFIERS);

        // execute
        DataSetReference metadata = converter.convert(sourceDataset);

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

    private static Dataset emptyEntityBuilder(String id) {
        Dataset dataset = new Dataset();
        dataset.setId(id);
        return dataset;
    }

    private static Result minimalEntityBuilder(String id, String... types) {
        Result result = emptyEntityBuilder(id);
        addPublicationTypes(result, types);
        return result;
    }

    private static Result documentEntity(ImmutableMap<String, String> extIdentifiers) {
        Result result = minimalEntityBuilder(ID,
                InfoSpaceConstants.SEMANTIC_CLASS_INSTANCE_TYPE_ARTICLE,
                InfoSpaceConstants.SEMANTIC_CLASS_INSTANCE_TYPE_DATASET);

        addTitle(result, TITLE, null);
        addTitle(result, OTHER_TITLE, null);
        
        addDescription(result, ABSTRACT);

        if (result.getPid() == null) {
            result.setPid(new ArrayList<>());
        }
        
        extIdentifiers.entrySet().stream().map(entry -> {
            StructuredProperty structPid = new StructuredProperty();
            structPid.setValue(entry.getValue());
            Qualifier pidQualifier = new Qualifier();
            pidQualifier.setClassid(entry.getKey());
            structPid.setQualifier(pidQualifier);
            return structPid;
        }).forEach(pid -> {
            result.getPid().add(pid);
            // testing for dealing with duplicates
            result.getPid().add(pid);
        });
        
        Field<String> dateOfAcc = new Field<>();
        dateOfAcc.setValue(String.format("%s-02-29", YEAR));
        result.setDateofacceptance(dateOfAcc);
        
        Field<String> publisher = new Field<>();
        publisher.setValue(PUBLISHER);
        result.setPublisher(publisher);
        
        Field<String> format = new Field<>();
        format.setValue(FORMAT);
        if (result.getFormat() == null) {
            result.setFormat(new ArrayList<>());
        }
        result.getFormat().add(format);

        result.setResulttype(buildQualifierWithClassId(RESULT_TYPE_CLASSID));
        result.setResourcetype(buildQualifierWithClassId(RESOURCE_TYPE_CLASSID));
        
        Author author = new Author();
        author.setFullname(FULL_NAME);
        author.setRank(0);
        if (result.getAuthor() == null) {
            result.setAuthor(new ArrayList<>());
        }
        result.getAuthor().add(author);
        
        return result;
    }
    
    private static Qualifier buildQualifierWithClassId(String classId) {
        Qualifier qualifier = new Qualifier();
        qualifier.setClassid(classId);
        return qualifier;
    }

    private static void addPublicationTypes(Result dataset, String... types) {
        Arrays.stream(types).forEach(type -> addPublicationType(dataset, type));
    }

    private static void addPublicationType(Result result, String publicationType) {
        if (result.getInstance()==null) {
            result.setInstance(new ArrayList<>());
        }
        Instance instance = new Instance();
        Qualifier instancetype = new Qualifier();
        instancetype.setClassid(publicationType);
        instance.setInstancetype(instancetype);
        result.getInstance().add(instance);
    }
    
    private static void addTitle(Result result, String title, String titleType) {
        if (result.getTitle()==null) {
            result.setTitle(new ArrayList<>());
        }
        StructuredProperty structTitle = new StructuredProperty();
        structTitle.setValue(title);
        if (titleType != null) {
            Qualifier titleQualifier = new Qualifier();
            titleQualifier.setClassid(titleType);
            structTitle.setQualifier(titleQualifier);
        }
        result.getTitle().add(structTitle);
    }

    private static void addDescription(Result result, String value) {
        if (result.getDescription()==null) {
            result.setDescription(new ArrayList<>());
        }
        Field<String> fieldDescr = new Field<>();
        fieldDescr.setValue(value);
        result.getDescription().add(fieldDescr);
    }

}
