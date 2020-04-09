package eu.dnetlib.iis.wf.importer.infospace.converter;

import com.google.common.base.Preconditions;

import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;

/**
 * Project-organization {@link eu.dnetlib.dhp.schema.oaf.Relation} relation to {@link ProjectToOrganization} converter.
 * 
 * @author mhorst
 *
 */
public class ProjectToOrganizationRelationConverter implements OafRelToAvroConverter<ProjectToOrganization> {

    private static final long serialVersionUID = -8783122604588939123L;

    /**
     * Builds {@link ProjectToOrganization} object from given {@link eu.dnetlib.dhp.schema.oaf.Relation} relation.
     */
    @Override
    public ProjectToOrganization convert(Relation relation) {
        Preconditions.checkNotNull(relation);
        ProjectToOrganization.Builder builder = ProjectToOrganization.newBuilder();
        builder.setProjectId(relation.getSource());
        builder.setOrganizationId(relation.getTarget());
        return builder.build();
    }
    
}
