package eu.dnetlib.iis.wf.importer.infospace.converter;

import java.io.IOException;
import java.io.Serializable;

import org.apache.avro.specific.SpecificRecord;

import eu.dnetlib.dhp.schema.oaf.Relation;

/**
 * {@link Relation} relation model to avro object converter.
 * 
 * @author mhorst
 *
 * @param <T> avro record type to be produced
 */
public interface OafRelToAvroConverter<T extends SpecificRecord> extends Serializable {

    /**
     * Builds avro objects from {@link Relation} relation input.
     * 
     * @param oafRelation {@link Relation} relation entity
     * @return avro object
     */
    T convert(Relation oafRelation) throws IOException;
}
