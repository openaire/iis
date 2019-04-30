package eu.dnetlib.iis.wf.referenceextraction.project;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileUtils;

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
        super(Project.SCHEMA$, "project", "project_db");
    }

    // -------------------------- LOGIC -------------------------------------

    @Override
    public ProcessExecutionContext initializeProcess(Map<String, String> parameters) throws IOException {
        String targetDbLocation = System.getProperty("java.io.tmpdir") + File.separatorChar + "base_projects.db";
        File targetDbFile = new File(targetDbLocation);
        FileUtils.copyFile(new File("scripts/base_projects.db"), targetDbFile);
        targetDbFile.setWritable(true);
        return new ProcessExecutionContext(
                Runtime.getRuntime().exec("/usr/local/sbin/pypy scripts/madis/mexec.py -d " + targetDbLocation + " -f scripts/buildprojectdb.sql"),
                targetDbFile);
    }
}
