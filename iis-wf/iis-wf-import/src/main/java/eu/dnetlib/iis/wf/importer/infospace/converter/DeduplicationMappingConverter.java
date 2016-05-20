package eu.dnetlib.iis.wf.importer.infospace.converter;

import com.google.common.base.Preconditions;

import eu.dnetlib.data.proto.OafProtos.OafRel;
import eu.dnetlib.iis.common.schemas.IdentifierMapping;

/**
 * Dedup {@link OafRel} relations to {@link IdentifierMapping} converter.
 * 
 * @author mhorst
 *
 */
public class DeduplicationMappingConverter implements OafRelToAvroConverter<IdentifierMapping> {

    @Override
    public IdentifierMapping convert(OafRel oafRel) {
        Preconditions.checkNotNull(oafRel);
        IdentifierMapping.Builder builder = IdentifierMapping.newBuilder();
        builder.setNewId(oafRel.getSource());
        builder.setOriginalId(oafRel.getTarget());
        return builder.build();
    }
}
