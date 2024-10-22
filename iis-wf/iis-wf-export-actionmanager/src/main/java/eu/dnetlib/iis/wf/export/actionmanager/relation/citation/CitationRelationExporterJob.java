package eu.dnetlib.iis.wf.export.actionmanager.relation.citation;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.wf.export.actionmanager.entity.ConfidenceLevelUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static eu.dnetlib.iis.common.spark.SparkSessionSupport.runWithSparkSession;
import static eu.dnetlib.iis.wf.export.actionmanager.relation.citation.CitationRelationExporterIOUtils.*;
import static eu.dnetlib.iis.wf.export.actionmanager.relation.citation.CitationRelationExporterUtils.*;
import static org.apache.spark.sql.functions.udf;

public class CitationRelationExporterJob {

    private CitationRelationExporterJob() {
    }

    private static final Logger logger = LoggerFactory.getLogger(CitationRelationExporterJob.class);

    public static void main(String[] args) {
        JobParameters params = new JobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        Float confidenceLevelThreshold = ConfidenceLevelUtils
                .evaluateConfidenceLevelThreshold(params.trustLevelThreshold);
        logger.info("Confidence level threshold to be used: {}.", confidenceLevelThreshold);
        
        final String collectedFromValue = params.collectedFromValue;

        runWithSparkSession(new SparkConf(), params.isSparkSessionShared, spark -> {
            clearOutput(spark, params.outputRelationPath, params.outputReportPath);

            Dataset<Row> citations = readCitations(spark, params.inputCitationsPath);

            UserDefinedFunction isValidConfidenceLevel = udf((UDF1<Float, Boolean>) confidenceLevel ->
                            ConfidenceLevelUtils.isValidConfidenceLevel(confidenceLevel, confidenceLevelThreshold),
                    DataTypes.BooleanType);
            Dataset<Relation> relations = processCitations(citations, isValidConfidenceLevel, collectedFromValue);
            relations.cache();

            Dataset<Text> serializedActions = relationsToSerializedActions(relations);
            storeSerializedActions(spark, serializedActions, params.outputRelationPath);

            Dataset<ReportEntry> reportEntries = relationsToReportEntries(spark, relations);
            storeReportEntries(spark, reportEntries, params.outputReportPath);
        });
    }

    @Parameters(separators = "=")
    private static class JobParameters {
        @Parameter(names = "-sharedSparkSession")
        private Boolean isSparkSessionShared = Boolean.FALSE;

        @Parameter(names = "-inputCitationsPath", required = true)
        private String inputCitationsPath;

        @Parameter(names = "-outputRelationPath", required = true)
        private String outputRelationPath;

        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;

        @Parameter(names = "-trustLevelThreshold", required = true)
        private String trustLevelThreshold;
        
        @Parameter(names = "-collectedFromValue", required = true)
        private String collectedFromValue;
    }
}
