package eu.dnetlib.iis.wf.importer.infospace.converter;

import java.io.IOException;

import org.apache.avro.specific.SpecificRecord;

import eu.dnetlib.data.proto.OafProtos.OafRel;

/**
 * {@link OafRel} relation model to avro object converter.
 * 
 * @author mhorst
 *
 * @param <T> avro record type to be produced
 */
public interface OafRelToAvroConverter<T extends SpecificRecord> {

    /**
     * Builds avro objects from {@link OafRel} relation input.
     * 
     * @param oafRelation {@link OafRel} relation entity
     * @return avro object
     */
    public T convert(OafRel oafRelation) throws IOException;
}
