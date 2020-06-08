package eu.dnetlib.iis.wf.referenceextraction.patent;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.audit.schemas.Fault;
import eu.dnetlib.iis.common.fault.FaultUtils;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.referenceextraction.patent.schemas.ImportedPatent;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeException;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeUtils;
import eu.dnetlib.iis.wf.referenceextraction.ContentRetrieverResponse;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

/**
 * Job responsible for retrieving full patent metadata via {@link PatentServiceFacade} based on {@link ImportedPatent} input.
 *  
 * @author mhorst
 *
 */
public class PatentMetadataRetrieverJob {
    
    private static final String COUNTER_PROCESSED_TOTAL = "processing.referenceExtraction.patent.retrieval.processed.total";
    
    private static final String COUNTER_PROCESSED_FAULT = "processing.referenceExtraction.patent.retrieval.processed.fault";

    private static final Logger log = Logger.getLogger(PatentMetadataRetrieverJob.class);
    
    private static final SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static final SparkAvroSaver avroSaver = new SparkAvroSaver();

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws IOException {
        JobParameters params = new JobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        OutputPaths outputPaths = new OutputPaths(params);
        
        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputFaultPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputReportPath);
            
            try {
                PatentServiceFacade patentServiceFacade = ServiceFacadeUtils.instantiate(sc.hadoopConfiguration());
                
                JavaRDD<ImportedPatent> importedPatents = avroLoader.loadJavaRDD(sc, params.inputPath,
                        ImportedPatent.class);

                Tuple2<JavaRDD<DocumentText>, JavaRDD<Fault>> results = obtainMetadata(importedPatents, patentServiceFacade);
                
                storeInOutput(results._1, results._2, generateReportEntries(sc, results._1, results._2), outputPaths,
                        params.numberOfEmittedFiles);
                
            } catch (ServiceFacadeException e) {
                throw new RuntimeException("unable to instantiate patent service facade!", e);
            }
        }
    }

    //------------------------ PRIVATE --------------------------

    private static JavaRDD<ReportEntry> generateReportEntries(JavaSparkContext sparkContext, 
            JavaRDD<DocumentText> processedEntities, JavaRDD<Fault> processedFaults) {
        
        Preconditions.checkNotNull(sparkContext, "sparkContext has not been set");
        
        ReportEntry processedEnttiesCounter = ReportEntryFactory.createCounterReportEntry(COUNTER_PROCESSED_TOTAL, processedEntities.count());
        ReportEntry processedFaultsCounter = ReportEntryFactory.createCounterReportEntry(COUNTER_PROCESSED_FAULT, processedFaults.count());
        
        return sparkContext.parallelize(Lists.newArrayList(processedEnttiesCounter, processedFaultsCounter));
    }
    
    private static void storeInOutput(JavaRDD<DocumentText> retrievedPatentMeta, 
            JavaRDD<Fault> faults, JavaRDD<ReportEntry> reports, OutputPaths outputPaths, int numberOfEmittedFiles) {
        avroSaver.saveJavaRDD(retrievedPatentMeta.coalesce(numberOfEmittedFiles), DocumentText.SCHEMA$, outputPaths.getResult());
        avroSaver.saveJavaRDD(faults.coalesce(numberOfEmittedFiles), Fault.SCHEMA$, outputPaths.getFault());
        avroSaver.saveJavaRDD(reports.coalesce(numberOfEmittedFiles), ReportEntry.SCHEMA$, outputPaths.getReport());
    }
    
    private static Tuple2<JavaRDD<DocumentText>, JavaRDD<Fault>> obtainMetadata(JavaRDD<ImportedPatent> input, PatentServiceFacade patentServiceFacade) {
        JavaPairRDD<CharSequence, ContentRetrieverResponse> obtainedIdToResponce = input
                .mapToPair(x -> new Tuple2<CharSequence, ContentRetrieverResponse>(getId(x),
                        obtainMetadata(x, patentServiceFacade)));
        obtainedIdToResponce.persist(StorageLevel.DISK_ONLY());
        return new Tuple2<>(
                obtainedIdToResponce.map(e -> DocumentText.newBuilder().setId(e._1).setText(e._2.getContent()).build()),
                obtainedIdToResponce.filter(e -> e._2.getException() != null).map(e -> FaultUtils.exceptionToFault(e._1, e._2.getException(), null)));
    }
        
    private static ContentRetrieverResponse obtainMetadata(ImportedPatent patent, PatentServiceFacade patentServiceFacade) {
        try {
            return new ContentRetrieverResponse(patentServiceFacade.getPatentMetadata(patent));
        } catch (Exception e) {
            log.error("Failed to obtain patent metadata for patent: " + patent.getApplnNr(), e);
            return new ContentRetrieverResponse(e);
        }
    }

    private static CharSequence getId(ImportedPatent patent) {
        return patent.getApplnNr();
    }
    
    private static class OutputPaths {
        
        private final String result;
        private final String fault;
        private final String report;
        
        public OutputPaths(JobParameters params) {
            this.result = params.outputPath;
            this.fault = params.outputFaultPath;
            this.report = params.outputReportPath;
        }

        public String getResult() {
            return result;
        }

        public String getFault() {
            return fault;
        }

        public String getReport() {
            return report;
        }
    }
    
    @Parameters(separators = "=")
    private static class JobParameters {
        @Parameter(names = "-inputPath", required = true)
        private String inputPath;

        @Parameter(names = "-numberOfEmittedFiles", required = true)
        private int numberOfEmittedFiles;
        
        @Parameter(names = "-outputPath", required = true)
        private String outputPath;
        
        @Parameter(names = "-outputFaultPath", required = true)
        private String outputFaultPath;
        
        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
        
    }
}
