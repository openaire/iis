package eu.dnetlib.iis.wf.referenceextraction.project.tara;

import eu.dnetlib.iis.common.spark.pipe.PipeExecutionEnvironment;
import org.apache.spark.SparkContext;

public class TaraPipeExecutionEnvironment implements PipeExecutionEnvironment {
    private SparkContext sc;
    private String scriptsDir;
    private String projectDbFile;

    public TaraPipeExecutionEnvironment(SparkContext sc, String scriptsDir, String projectDbFile) {
        this.sc = sc;
        this.scriptsDir = scriptsDir;
        this.projectDbFile = projectDbFile;
    }

    @Override
    public String pipeCommand() {
        sc.addFile(scriptsDir, true);
        sc.addFile(projectDbFile);
        return "bash scripts/run_tara_referenceextraction.sh";
    }
}
