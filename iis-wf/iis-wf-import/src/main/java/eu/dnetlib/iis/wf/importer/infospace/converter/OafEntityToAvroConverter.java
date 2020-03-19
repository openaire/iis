package eu.dnetlib.iis.wf.importer.infospace.converter;

import java.io.IOException;

import org.apache.avro.specific.SpecificRecord;

import eu.dnetlib.dhp.schema.oaf.OafEntity;

/**
 * {@link OafEntity} entity to avro object converter.
 * 
 * @author mhorst
 *
 * @param <Target> avro record type to be produced
 */
public interface OafEntityToAvroConverter<Source extends OafEntity, Target extends SpecificRecord> {

    /**
     * Builds avro objects from {@link OafEntity} entity.
     * 
     * @param oafEntity {@link OafEntity} main entity
     * @return avro object
     */
    Target convert(Source oafRecord) throws IOException;
}
