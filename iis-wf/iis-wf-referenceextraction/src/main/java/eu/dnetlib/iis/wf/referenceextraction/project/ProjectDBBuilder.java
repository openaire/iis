package eu.dnetlib.iis.wf.referenceextraction.project;

import eu.dnetlib.iis.importer.schemas.Project;
import eu.dnetlib.iis.wf.referenceextraction.AbstractDBBuilder;

/**
 * Process supplementing existing projects database reading {@link Project} input avro records.
 *
 * @author mhorst
 */
public class ProjectDBBuilder extends AbstractDBBuilder<Project> {

    // -------------------------- CONSTRUCTORS ------------------------------

    public ProjectDBBuilder() {
        super(Project.SCHEMA$);
    }

}
