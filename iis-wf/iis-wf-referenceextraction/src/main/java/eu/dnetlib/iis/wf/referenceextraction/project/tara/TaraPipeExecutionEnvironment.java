package eu.dnetlib.iis.wf.referenceextraction.project.tara;

import eu.dnetlib.iis.common.spark.pipe.PipeExecutionEnvironment;
import org.apache.spark.SparkContext;

/**
 * Abstraction of cluster execution environment for TARA project reference extraction.
 * <p>
 * Madis scripts dir and SQLite projects db file will be propagated to cluster worker nodes upon creation  of this
 * class. String command to be run by RDD 'pipe' method executes TARA reference extraction in the context of each
 * node's worker dir.
 */
public class TaraPipeExecutionEnvironment implements PipeExecutionEnvironment {

    public TaraPipeExecutionEnvironment(SparkContext sc, String scriptsDir, String projectDbFile) {
        sc.addFile(scriptsDir, true);
        sc.addFile(projectDbFile);
    }

    @Override
    public String pipeCommand() {
        return "bash scripts/run_tara_referenceextraction.sh";
    }
}
