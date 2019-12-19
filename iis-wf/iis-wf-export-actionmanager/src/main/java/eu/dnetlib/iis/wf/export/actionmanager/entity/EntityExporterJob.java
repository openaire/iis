package eu.dnetlib.iis.wf.export.actionmanager.entity;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import eu.dnetlib.actionmanager.actions.ActionFactory;
import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.actions.XsltInfoPackageAction;
import eu.dnetlib.actionmanager.common.Operation;
import eu.dnetlib.actionmanager.common.Provenance;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.wf.export.actionmanager.cfg.StaticConfigurationProvider;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import scala.Tuple2;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Generic entity exporter reading XML records and exporting them as actions.
 * 
 * @author mhorst
 *
 */
public class EntityExporterJob {
    
    private static final Provenance PROVENANCE_DEFAULT = Provenance.sysimport_mining_repository;
    
    private static final String ENTITY_XSLT_NAME = "entity2actions";
    
    private static EntityExportCounterReporter counterReporter = new EntityExportCounterReporter();

    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws Exception {
        EntityExporterJobParameters params = new EntityExporterJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputAvroPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputReportPath);
            
            EntityFilter entityProvider = buildEntityProvider(params.entityFilterClassName);
            
            Float trustLevelThreshold = null;
            if (StringUtils.isNotBlank(params.trustLevelThreshold) && 
                    !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(params.trustLevelThreshold)) {
                trustLevelThreshold = Float.valueOf(params.trustLevelThreshold);
            }
            
            JavaRDD<CharSequence> entityText = entityProvider.provideRDD(sc, 
                    params.inputRelationAvroPath, params.inputEntityAvroPath, trustLevelThreshold);
            entityText.cache();
            
            // need to get values as serializable objects before passing them to map method
            String actionSetId = params.actionSetId;
            String entityXSLTLocation = params.entityXSLTLocation;

            JavaPairRDD<Text, Text> result = entityText
                    .flatMapToPair(x ->
                            generateActions(x.toString(), actionSetId, buildActionFactory(ENTITY_XSLT_NAME, entityXSLTLocation), ENTITY_XSLT_NAME).stream()
                                    .map(action -> new Tuple2<>(new Text(action.getRowKey()), new Text(action.toString())))
                                    .collect(Collectors.toList()).iterator());

            counterReporter.report(sc, entityText, params.outputReportPath, params.counterName);
            
            // enabling block compression using default algorithm
            Job job = Job.getInstance();
            job.getConfiguration().set(FileOutputFormat.COMPRESS, Boolean.TRUE.toString());
            job.getConfiguration().set(FileOutputFormat.COMPRESS_TYPE, CompressionType.BLOCK.name());
            result.saveAsNewAPIHadoopFile(params.outputAvroPath, Text.class, Text.class, SequenceFileOutputFormat.class, job.getConfiguration());
        }
    }

    // ----------------------------------------- PRIVATE ----------------------------------------------
    
    /**
     * Builds entity provider instance of a class provided as parameter.
     */
    private static EntityFilter buildEntityProvider(String entityProviderClassName) throws Exception {
        Class<?> clazz = Class.forName(entityProviderClassName);
        Constructor<?> ctor = clazz.getConstructor();
        return (EntityFilter) ctor.newInstance();
    }
    
    /**
     * Creates action factory transforming XML OAI records into actions.
     * 
     */
    private static ActionFactory buildActionFactory(String entityXSLTName, String entityXSLTLocation) {
        Map<String, Resource> xslts = new HashMap<String, Resource>();
        xslts.put(entityXSLTName, new ClassPathResource(entityXSLTLocation));
        ActionFactory localActionFactory = new ActionFactory();
        localActionFactory.setXslts(xslts);
        return localActionFactory;
    }
    
    /**
     * Handles single record retrieved from MDStore.
     * 
     * @param oaiRecord OAI XML record to be processed
     * @param actionSetId action set identifier to be attached to generated actions
     * @param actionManager generated actions consumer
     */
    private static List<AtomicAction> generateActions(String oaiRecord, String actionSetId, ActionFactory actionFactory,
            String entityXSLTName) throws Exception {

        XsltInfoPackageAction xsltAction = actionFactory.generateInfoPackageAction(entityXSLTName, actionSetId,
                StaticConfigurationProvider.AGENT_DEFAULT, Operation.INSERT, oaiRecord, PROVENANCE_DEFAULT,
                StaticConfigurationProvider.ACTION_TRUST_0_9);
        
        return xsltAction.asAtomicActions();
    }
    
    @Parameters(separators = "=")
    private static class EntityExporterJobParameters {
        
        @Parameter(names = "-inputRelationAvroPath", required = true)
        private String inputRelationAvroPath;
        
        @Parameter(names = "-inputEntityAvroPath", required = true)
        private String inputEntityAvroPath;
        
        @Parameter(names = "-outputAvroPath", required = true)
        private String outputAvroPath;
        
        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
        
        @Parameter(names = "-entityFilterClassName", required = true)
        private String entityFilterClassName;
        
        @Parameter(names = "-entityXSLTLocation", required = true)
        private String entityXSLTLocation;
        
        @Parameter(names = "-actionSetId", required = true)
        private String actionSetId;
        
        @Parameter(names = "-trustLevelThreshold", required = false)
        private String trustLevelThreshold;
        
        @Parameter(names = "-counterName", required = true)
        private String counterName;
    }
    
}
