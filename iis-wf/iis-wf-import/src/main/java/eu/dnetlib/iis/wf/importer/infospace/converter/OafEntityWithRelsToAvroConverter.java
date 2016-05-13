package eu.dnetlib.iis.wf.importer.infospace.converter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.avro.specific.SpecificRecord;

import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.data.proto.OafProtos.OafRel;
import eu.dnetlib.iis.wf.importer.infospace.QualifiedOafJsonRecord;

/**
 * {@link OafEntity} with associated {@link OafRel} relation objects to avro converter.
 * 
 * @author mhorst
 *
 * @param <T>
 */
public interface OafEntityWithRelsToAvroConverter<T extends SpecificRecord> {

    /**
     * Builds avro objects from {@link Oaf} entity.
     * 
     * @param oafEntity {@link Oaf} main entity
     * @param relations relations associated with main entity. Records are mapped by column family.
     * @return avro object
     * @throws IOException
     */
    public T convert(OafEntity oafEntity, Map<String, List<QualifiedOafJsonRecord>> relations) throws IOException;
}
