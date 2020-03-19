package eu.dnetlib.iis.wf.importer.infospace.converter;

import com.google.common.base.Preconditions;

import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.common.schemas.IdentifierMapping;

/**
 * Dedup {@link Relation} relations to {@link IdentifierMapping} converter.
 * 
 * @author mhorst
 *
 */
public class DeduplicationMappingConverter implements OafRelToAvroConverter<IdentifierMapping> {

    @Override
    public IdentifierMapping convert(Relation relation) {
        Preconditions.checkNotNull(relation);
        IdentifierMapping.Builder builder = IdentifierMapping.newBuilder();
        builder.setNewId(relation.getSource());
        builder.setOriginalId(relation.getTarget());
        return builder.build();
    }
}
