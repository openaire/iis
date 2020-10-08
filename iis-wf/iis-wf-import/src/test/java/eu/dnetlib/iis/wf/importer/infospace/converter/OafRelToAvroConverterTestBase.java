package eu.dnetlib.iis.wf.importer.infospace.converter;

import eu.dnetlib.dhp.schema.oaf.Relation;
import org.apache.avro.specific.SpecificRecord;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Base class for tests of the various {@link OafRelToAvroConverter}s
 *
 * @param <T> converter result type
 */
public abstract class OafRelToAvroConverterTestBase<T extends SpecificRecord> {

    private static final String SOURCE_ID = "a source identifier";
    private static final String TARGET_ID = "identifier of the target";

    /**
     * The converter under test
     */
    protected OafRelToAvroConverter<T> converter;

    /**
     * Source id getter in the Avro class
     */
    protected Function<T, CharSequence> getSourceId;

    /**
     * Target id getter in the Avro class
     */
    protected Function<T, CharSequence> getTargetId;

    // ------------------------ TESTS --------------------------

    @Test
    public void convert_null_oafRel() {
        // execute
        assertThrows(NullPointerException.class, () -> converter.convert(null));
    }

    @Test
    public void convert() throws Exception {
        // given
        Relation oafRel = createOafRelObject();

        // execute
        T rel = converter.convert(oafRel);

        // assert
        assertNotNull(rel);
        assertEquals(SOURCE_ID, getSourceId.apply(rel));
        assertEquals(TARGET_ID, getTargetId.apply(rel));
    }

    /**
     * Creates {@link Relation} instance with source and target identifiers set.
     */
    public static Relation createOafRelObject(String sourceId, String targetId) {
        Relation rel = new Relation();
        rel.setSource(sourceId);
        rel.setTarget(targetId);
        rel.setRelType("resultResult");
        rel.setSubRelType("provision");
        rel.setRelClass("irrelevantRelClass");
        return rel;
    }
    
    //------------------------ PRIVATE --------------------------

    private static Relation createOafRelObject() {
        return createOafRelObject(SOURCE_ID, TARGET_ID);
    }
}
