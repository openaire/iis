package eu.dnetlib.iis.wf.importer.infospace.converter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.importer.schemas.DocumentMetadata;
import eu.dnetlib.iis.wf.importer.infospace.approver.FieldApprover;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * {@link DocumentMetadataConverter} test class.
 */
@ExtendWith(MockitoExtension.class)
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
    private static final ImmutableList<String> DATASOURCE_IDS = ImmutableList.of("source id 1", "id #2");

    private static final String FIRST_NAME = "a first name";
    private static final String SECOND_NAME = "a second name";
    private static final String SECOND_SECOND_NAME = "another name";
    private static final String FULL_NAME = "the full name";
    
    @InjectMocks
    private DocumentMetadataConverter converter;

    @Mock
    private FieldApprover fieldApprover;

    @BeforeEach
    public void setUp() {
        lenient().when(fieldApprover.approve(any())).thenReturn(true);
    }

    // ------------------------ TESTS --------------------------

    @Test
    public void convert_null_oafEntity() {
        // execute
        assertThrows(NullPointerException.class, () -> converter.convert(null));
    }

    @Test
    public void convert_using_main_title() throws IOException {
        // given
        Publication publication = minimalEntityBuilder(ID, InfoSpaceConstants.SEMANTIC_CLASS_INSTANCE_TYPE_ARTICLE);

        addTitle(publication, OTHER_TITLE);
        addTitle(publication, TITLE, InfoSpaceConstants.SEMANTIC_CLASS_MAIN_TITLE);

        // execute
        DocumentMetadata metadata = converter.convert(publication);

        // assert
        assertEquals(TITLE, metadata.getTitle());
    }
    
    @Test
    public void convert_using_null_main_title_and_not_null_other_title() throws IOException {
     // given
        Publication publication = minimalEntityBuilder(ID, InfoSpaceConstants.SEMANTIC_CLASS_INSTANCE_TYPE_ARTICLE);

        addTitle(publication, OTHER_TITLE);
        addTitle(publication, null, InfoSpaceConstants.SEMANTIC_CLASS_MAIN_TITLE);

        // execute
        DocumentMetadata metadata = converter.convert(publication);

        // assert
        assertEquals(OTHER_TITLE, metadata.getTitle());
    }

    @Test
    public void convert_using_first_title() throws IOException {
        // given
        Publication publication = minimalEntityBuilder(ID, InfoSpaceConstants.SEMANTIC_CLASS_INSTANCE_TYPE_ARTICLE);

        addTitle(publication, OTHER_TITLE);
        addTitle(publication, TITLE);

        // execute
        DocumentMetadata metadata = converter.convert(publication);

        // assert
        assertEquals(OTHER_TITLE, metadata.getTitle());
    }
    
    @Test
    public void convert_null_date_of_acceptance() throws IOException {
        // given
        Publication oafEntity = documentEntity();
        oafEntity.getDateofacceptance().setValue(null);
        
        // execute
        DocumentMetadata metadata = converter.convert(oafEntity);

        // assert
        assertNotNull(metadata);
        assertEquals(ID, metadata.getId());
        
        assertNull(metadata.getYear());
    }

    @Test
    public void convert_skip_null_abstract() throws IOException {
        // given
        Publication publication = minimalEntityBuilder(ID, InfoSpaceConstants.SEMANTIC_CLASS_INSTANCE_TYPE_ARTICLE);

        addDescription(publication, "null");
        addDescription(publication, ABSTRACT);

        // execute
        DocumentMetadata metadata = converter.convert(publication);

        // assert
        assertEquals(ABSTRACT, metadata.getAbstract$());
    }

    @Test
    public void convert_with_undefined_language() throws IOException {
        // given
        Publication publication = minimalEntityBuilder(ID, InfoSpaceConstants.SEMANTIC_CLASS_INSTANCE_TYPE_ARTICLE);

        setLanguage(publication, "und");

        // execute
        DocumentMetadata metadata = converter.convert(publication);

        // assert
        assertNull(metadata.getLanguage());
    }


    @Test
    public void convert_not_approved() throws IOException {
        // given
        Publication oafEntity = documentEntity();

        when(fieldApprover.approve(any())).thenReturn(false);

        // execute
        DocumentMetadata metadata = converter.convert(oafEntity);

        // assert
        assertEquals(ID, metadata.getId());
        assertNull(metadata.getTitle());
        assertNull(metadata.getAbstract$());
        assertEquals(LANGUAGE, metadata.getLanguage());
        assertNull(metadata.getKeywords());
        assertNull(metadata.getExternalIdentifiers());
        assertNull(metadata.getJournal());
        assertNull(metadata.getYear());
        assertNull(metadata.getPublisher());
        assertTrue(metadata.getPublicationType().getArticle());
        assertTrue(metadata.getPublicationType().getDataset());
        assertEquals(DATASOURCE_IDS, metadata.getDatasourceIds());
        
        assertEquals(1, metadata.getAuthors().size());
        assertEquals(FIRST_NAME, metadata.getAuthors().get(0).getName());
        assertEquals(SECOND_NAME + ' ' + SECOND_SECOND_NAME, metadata.getAuthors().get(0).getSurname());
        assertEquals(FULL_NAME, metadata.getAuthors().get(0).getFullname());
    }

    @Test
    public void convert() throws IOException {
        // given
        Publication oafEntity = documentEntity();

        // execute
        DocumentMetadata metadata = converter.convert(oafEntity);

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
        
        assertEquals(1, metadata.getAuthors().size());
        assertEquals(FIRST_NAME, metadata.getAuthors().get(0).getName());
        assertEquals(SECOND_NAME + ' ' + SECOND_SECOND_NAME, metadata.getAuthors().get(0).getSurname());
        assertEquals(FULL_NAME, metadata.getAuthors().get(0).getFullname());
        
        assertEquals(DATASOURCE_IDS, metadata.getDatasourceIds());
    }

    // ------------------------ PRIVATE --------------------------

    private static Publication emptyEntityBuilder(String id) {
        Publication result = new Publication();
        result.setId(id);
        return result;
    }

    private static Publication minimalEntityBuilder(String id, String... types) {
        Publication result = emptyEntityBuilder(id);
        addPublicationTypes(result, types);
        return result;
    }

    private static Publication documentEntity() {
        Publication result = minimalEntityBuilder(ID,
                InfoSpaceConstants.SEMANTIC_CLASS_INSTANCE_TYPE_ARTICLE,
                InfoSpaceConstants.SEMANTIC_CLASS_INSTANCE_TYPE_DATASET);

        addTitle(result, TITLE);
        addDescription(result, ABSTRACT);
        setLanguage(result, LANGUAGE);

        result.setSubject(Lists.newArrayList());
        KEYWORDS.stream().map(keyword -> {
            Subject subject = new Subject();
            subject.setValue(keyword);
            return subject;
        }).forEach(result.getSubject()::add);

        result.getInstance().forEach(instance -> instance.setPid(Lists.newArrayList()));
        EXT_IDENTIFIERS.entrySet().stream().map(entry -> {
            StructuredProperty pid = new StructuredProperty();
            pid.setValue(entry.getValue());
            Qualifier pidType = new Qualifier();
            pidType.setClassid(entry.getKey());
            pid.setQualifier(pidType);
            return pid;
        }).forEach(pid -> result.getInstance().forEach(instance -> instance.getPid().add(pid)));
        
        Journal journal = new Journal();
        journal.setName(JOURNAL);
        result.setJournal(journal);
        
        Field<String> dateofacceptance = new Field<>();
        dateofacceptance.setValue(String.format("%s-02-29", YEAR));
        result.setDateofacceptance(dateofacceptance);

        Field<String> publisher = new Field<>();
        publisher.setValue(PUBLISHER);
        result.setPublisher(publisher);

        result.setCollectedfrom(Lists.newArrayList());
        DATASOURCE_IDS.stream().map(id -> {
            KeyValue collFrom = new KeyValue();
            collFrom.setKey(id);
            return collFrom;
        }).forEach(result.getCollectedfrom()::add);
        
        Author author = new Author();
        author.setName(FIRST_NAME);
        author.setSurname(SECOND_NAME + ' ' + SECOND_SECOND_NAME);
        author.setFullname(FULL_NAME);
        author.setRank(0);
        result.setAuthor(Lists.newArrayList(author));
        
        return result;
    }

    private static void addPublicationTypes(Result result, String... types) {
        if (types.length > 0) {
            if (result.getInstance() == null) {
                result.setInstance(Lists.newArrayList());
            }
            Arrays.asList(types).stream().map(type -> {
                Instance instance = new Instance();
                Qualifier instanceType = new Qualifier();
                instanceType.setClassid(type);
                instance.setInstancetype(instanceType);
                return instance;
            }).forEach(result.getInstance()::add);
        }
    }

    private static void addTitle(Result result, String value) {
        addTitle(result, value, null);
    }
    
    private static void addTitle(Result result, String value, String type) {
        if (result.getTitle() == null) {
            result.setTitle(Lists.newArrayList());
        }
        StructuredProperty title = new StructuredProperty();
        title.setValue(value);
        if (type != null) {
            Qualifier titleType = new Qualifier();
            titleType.setClassid(type);
            title.setQualifier(titleType);
        }
        result.getTitle().add(title);
    }

    private static void addDescription(Result result, String value) {
        if (result.getDescription() == null) {
            result.setDescription(Lists.newArrayList());
        }
        Field<String> descr = new Field<>();
        descr.setValue(value);
        result.getDescription().add(descr);
    }

    private static void setLanguage(Result result, String value) {
        Qualifier lang = new Qualifier();
        lang.setClassid(value);
        result.setLanguage(lang);
    }

}
