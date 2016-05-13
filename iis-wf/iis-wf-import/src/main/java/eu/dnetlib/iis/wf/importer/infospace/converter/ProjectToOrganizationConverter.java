package eu.dnetlib.iis.wf.importer.infospace.converter;

import com.google.common.base.Preconditions;

import eu.dnetlib.data.proto.OafProtos.OafRel;
import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;

/**
 * Project-organization {@link OafRel} relation to {@link ProjectToOrganization} converter.
 * 
 * @author mhorst
 *
 */
public class ProjectToOrganizationConverter implements OafRelToAvroConverter<ProjectToOrganization> {

    /**
     * Builds {@link ProjectToOrganization} object from given {@link OafRel} relation.
     */
    @Override
    public ProjectToOrganization convert(OafRel oafRel) {
        Preconditions.checkNotNull(oafRel);
        ProjectToOrganization.Builder builder = ProjectToOrganization.newBuilder();
        builder.setProjectId(oafRel.getSource());
        builder.setOrganizationId(oafRel.getTarget());
        return builder.build();
    }
}
