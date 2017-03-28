package eu.dnetlib.iis.wf.importer.infospace.converter;

import java.io.IOException;

import org.apache.avro.specific.SpecificRecord;

import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafEntity;

/**
 * {@link Oaf} entity to avro object converter.
 * 
 * @author mhorst
 *
 * @param <T> avro record type to be produced
 */
public interface OafEntityToAvroConverter<T extends SpecificRecord> {

    /**
     * Builds avro objects from {@link Oaf} entity.
     * 
     * @param oafEntity {@link Oaf} main entity
     * @return avro object
     */
    T convert(OafEntity oafEntity) throws IOException;
}
