package eu.dnetlib.iis.wf.referenceextraction.tara;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.importer.schemas.Project;
import eu.dnetlib.iis.wf.referenceextraction.AbstractDBBuilder;

/**
 * Process creating TARA projects database reading {@link Project} input avro records.
 *
 * @author mhorst
 */
public class TaraProjectDBBuilder extends AbstractDBBuilder<Project> {
    
    private final static String PARAM_SCRIPT_LOCATION = "scriptLocation";

    // -------------------------- CONSTRUCTORS ------------------------------

    public TaraProjectDBBuilder() {
        super(Project.SCHEMA$, "project", "project_db");
    }

    // -------------------------- LOGIC -------------------------------------

    @Override
    public ProcessExecutionContext initializeProcess(Map<String, String> parameters) throws IOException {
        String scriptLocation = parameters.get(PARAM_SCRIPT_LOCATION);
        Preconditions.checkArgument(StringUtils.isNotBlank(scriptLocation),
                "sql script location not provided, '%s' parameter is missing!", PARAM_SCRIPT_LOCATION);

        String targetDbLocation = System.getProperty("java.io.tmpdir") + File.separatorChar + "taraprojects.db";
        File targetDbFile = new File(targetDbLocation);
        targetDbFile.setWritable(true);

        return new ProcessExecutionContext(
                Runtime.getRuntime().exec("python scripts/madis/mexec.py -w " + targetDbLocation + " -f " + scriptLocation),
                targetDbFile);
        
    }
}
