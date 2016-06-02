package eu.dnetlib.iis.wf.importer.infospace.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.function.Function;

import org.apache.avro.specific.SpecificRecord;
import org.junit.Test;

import eu.dnetlib.data.proto.OafProtos.OafRel;
import eu.dnetlib.data.proto.RelTypeProtos.RelType;
import eu.dnetlib.data.proto.RelTypeProtos.SubRelType;

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

    @Test(expected = NullPointerException.class)
    public void convert_null_oafRel() throws Exception {
        // execute
        converter.convert(null);
    }

    @Test
    public void convert() throws Exception {
        // given
        OafRel oafRel = createOafRelObject();

        // execute
        T rel = converter.convert(oafRel);

        // assert
        assertNotNull(rel);
        assertEquals(SOURCE_ID, getSourceId.apply(rel));
        assertEquals(TARGET_ID, getTargetId.apply(rel));
    }

    //------------------------ PRIVATE --------------------------

    private static OafRel createOafRelObject() {
        return OafRel.newBuilder().setSource(SOURCE_ID).setTarget(TARGET_ID)
                // the following values do not matter for the converters but the fields are required
                .setRelType(RelType.resultResult).setSubRelType(SubRelType.provision)
                .setRelClass("irrelevantRelClass").setChild(false)
                .build();
    }
}
