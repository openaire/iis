package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.SoftwareHeritageOrigin;
import eu.dnetlib.iis.wf.referenceextraction.AbstractDBBuilder;

/**
 * Process building software heritage origins database reading {@link SoftwareHeritageOrigin} input avro records.
 *
 * @author mhorst
 */
public class SoftwareHeritageOriginDBBuilder extends AbstractDBBuilder<SoftwareHeritageOrigin> {

    private final static String PARAM_SCRIPT_LOCATION = "scriptLocation";

    // -------------------------- CONSTRUCTORS ------------------------------

    public SoftwareHeritageOriginDBBuilder() {
        super(SoftwareHeritageOrigin.SCHEMA$, "origins", "origins_db");
    }

    // -------------------------- LOGIC -------------------------------------

    @Override
    public ProcessExecutionContext initializeProcess(Map<String, String> parameters) throws IOException {
        String scriptLocation = parameters.get(PARAM_SCRIPT_LOCATION);
        Preconditions.checkArgument(StringUtils.isNotBlank(scriptLocation),
                "sql script location not provided, '%s' parameter is missing!", PARAM_SCRIPT_LOCATION);

        String targetDbLocation = System.getProperty("java.io.tmpdir") + File.separatorChar + "origins.db";
        File targetDbFile = new File(targetDbLocation);
        targetDbFile.setWritable(true);

        return new ProcessExecutionContext(
                Runtime.getRuntime().exec("python3 scripts/madis/mexec.py -w " + targetDbLocation + " -f " + scriptLocation),
                targetDbFile);
    }

}
