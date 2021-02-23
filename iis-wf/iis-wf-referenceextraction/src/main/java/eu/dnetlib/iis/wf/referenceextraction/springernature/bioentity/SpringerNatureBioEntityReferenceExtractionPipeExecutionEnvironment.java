package eu.dnetlib.iis.wf.referenceextraction.springernature.bioentity;

import eu.dnetlib.iis.common.spark.pipe.PipeExecutionEnvironment;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;

import java.io.IOException;

//todo: add javadoc
public class SpringerNatureBioEntityReferenceExtractionPipeExecutionEnvironment implements PipeExecutionEnvironment {

    private final String bioEntityDbFileName;
    private final String scriptName;

    public SpringerNatureBioEntityReferenceExtractionPipeExecutionEnvironment(SparkContext sc,
                                                                              String scriptsDir,
                                                                              String bioEntityDbFile) {
        this.scriptName = "springer.sql";
        this.bioEntityDbFileName = new Path(bioEntityDbFile).getName();

        sc.addFile(scriptsDir, true);
        sc.addFile(bioEntityDbFile);
    }

    @Override
    public String pipeCommand() throws IOException {
        return String.format("bash scripts/run_referenceextraction.sh %s %s", bioEntityDbFileName, scriptName);
    }
}
