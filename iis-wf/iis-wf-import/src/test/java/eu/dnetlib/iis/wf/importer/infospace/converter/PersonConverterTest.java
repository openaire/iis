package eu.dnetlib.iis.wf.importer.infospace.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.data.proto.OafProtos.OafEntity.Builder;
import eu.dnetlib.data.proto.PersonProtos.Person.Metadata;
import eu.dnetlib.data.proto.TypeProtos.Type;
import eu.dnetlib.iis.importer.schemas.Person;

/**
 * Tests for {@link PersonConverter}
 */
public class PersonConverterTest {

    private static final String PERSON_ID = "some id";
    private static final String FIRST_NAME = "a first name";
    private static final String SECOND_NAME = "a second name";
    private static final String SECOND_SECOND_NAME = "another name";
    private static final String FULL_NAME = "the full name";

    private PersonConverter converter = new PersonConverter();

    //------------------------ TESTS --------------------------

    @Test(expected=NullPointerException.class)
    public void convert_null_oafEntity() {

        // execute
        converter.convert(null);
    }

    @Test
    public void convert_unset_person() {
        // given
        OafEntity oafEntity = emptyEntityBuilder(PERSON_ID).build();

        // execute
        Person person = converter.convert(oafEntity);

        // assert
        assertNull(person);
    }

    @Test
    public void convert() {
        // given
        OafEntity.Builder builder = emptyEntityBuilder(PERSON_ID);

        Metadata.Builder mdBuilder = builder.getPersonBuilder().getMetadataBuilder();
        mdBuilder.getFirstnameBuilder().setValue(FIRST_NAME);
        mdBuilder.addSecondnamesBuilder().setValue(SECOND_NAME);
        mdBuilder.addSecondnamesBuilder().setValue(SECOND_SECOND_NAME);
        mdBuilder.getFullnameBuilder().setValue(FULL_NAME);

        OafEntity oafEntity = builder.build();

        // execute
        Person person = converter.convert(oafEntity);

        // assert
        assertEquals(PERSON_ID, person.getId());
        assertEquals(FIRST_NAME, person.getFirstname());
        assertEquals(2, person.getSecondnames().size());
        assertEquals(SECOND_NAME, person.getSecondnames().get(0));
        assertEquals(SECOND_SECOND_NAME, person.getSecondnames().get(1));
        assertEquals(FULL_NAME, person.getFullname());
    }

    //------------------------ PRIVATE --------------------------

    private static Builder emptyEntityBuilder(String id) {
        // note that the type does not matter for the converter
        return OafEntity.newBuilder().setType(Type.person).setId(id);
    }
}
