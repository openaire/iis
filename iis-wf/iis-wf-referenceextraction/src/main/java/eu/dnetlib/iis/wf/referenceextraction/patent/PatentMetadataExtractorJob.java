package eu.dnetlib.iis.wf.referenceextraction.patent;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

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
import eu.dnetlib.iis.referenceextraction.patent.schemas.Patent;
import eu.dnetlib.iis.wf.referenceextraction.patent.parser.OpsPatentMetadataXPathBasedParser;
import eu.dnetlib.iis.wf.referenceextraction.patent.parser.PatentMetadataParser;
import eu.dnetlib.iis.wf.referenceextraction.patent.parser.PatentMetadataParserException;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

/**
 * Job responsible for extracting {@link Patent} metadata out of the XML file
 * obtained from EPO endpoint.
 * 
 * @author mhorst
 *
 */
public class PatentMetadataExtractorJob {

    protected static final String COUNTER_PROCESSED_TOTAL = "processing.referenceExtraction.patent.metadataextraction.processed.total";
    
    protected static final String COUNTER_PROCESSED_FAULT = "processing.referenceExtraction.patent.metadataextraction.processed.fault";

    
    private static final Logger log = Logger.getLogger(PatentMetadataExtractorJob.class);

    private static final SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static final SparkAvroSaver avroSaver = new SparkAvroSaver();

    // ------------------------ LOGIC --------------------------

    public static void main(String[] args) throws IOException {
        JobParameters params = new JobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputFaultPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputReportPath);
            
            PatentMetadataParser parser = new OpsPatentMetadataXPathBasedParser();
            
            JavaRDD<ImportedPatent> importedPatent = avroLoader.loadJavaRDD(sc, params.inputImportedPatentPath, ImportedPatent.class);
            JavaRDD<DocumentText> toBeProcessedContents = avroLoader.loadJavaRDD(sc, params.inputDocumentTextPath, DocumentText.class);
            
            JavaPairRDD<CharSequence, Tuple2<DocumentText, ImportedPatent>> pairedInput = toBeProcessedContents
                    .mapToPair(x -> new Tuple2<CharSequence, DocumentText>(x.getId(), x))
                    .join(importedPatent.mapToPair(x -> new Tuple2<CharSequence, ImportedPatent>(x.getApplnNr(), x)));
            
            JavaRDD<PatentOrFault> parsedPatents = pairedInput.map(x -> parse(x._2._1, x._2._2, parser));
            parsedPatents.cache();
            
            Tuple2<JavaRDD<Patent>, JavaRDD<Fault>> splitPatentAndFault = splitPatentAndFault(parsedPatents);
            
            avroSaver.saveJavaRDD(splitPatentAndFault._1, Patent.SCHEMA$, params.outputPath);
            avroSaver.saveJavaRDD(splitPatentAndFault._2, Fault.SCHEMA$, params.outputFaultPath);
            avroSaver.saveJavaRDD(generateReportEntries(sc, splitPatentAndFault), ReportEntry.SCHEMA$,
                    params.outputReportPath);
        }
    }

    // ------------------------ PRIVATE --------------------------

    private static JavaRDD<ReportEntry> generateReportEntries(JavaSparkContext sparkContext, 
            Tuple2<JavaRDD<Patent>, JavaRDD<Fault>> splitPatentAndFault) {
        
        Preconditions.checkNotNull(sparkContext, "sparkContext has not been set");
        
        ReportEntry processedPatentsCounter = ReportEntryFactory.createCounterReportEntry(COUNTER_PROCESSED_TOTAL, splitPatentAndFault._1.count());
        ReportEntry processedFaultsCounter = ReportEntryFactory.createCounterReportEntry(COUNTER_PROCESSED_FAULT, splitPatentAndFault._2.count());
        
        return sparkContext.parallelize(Lists.newArrayList(processedPatentsCounter, processedFaultsCounter));
    }
    
    private static Tuple2<JavaRDD<Patent>, JavaRDD<Fault>> splitPatentAndFault(JavaRDD<PatentOrFault> parsedPatents) {
        return new Tuple2<>(parsedPatents.map(e -> e.getPatent()), 
                parsedPatents.filter(e -> e.getException() != null).map(
                        e -> FaultUtils.exceptionToFault(e.getPatent().getApplnNr(), e.getException(), null)));
    }
    
    private static PatentOrFault parse(DocumentText patent, ImportedPatent importedPatent, PatentMetadataParser parser) {
        Patent.Builder resultBuilder = fillDataFromImport(Patent.newBuilder(), importedPatent);
        try {
            return new PatentOrFault(parser.parse(patent.getText(), resultBuilder).build());
        } catch (PatentMetadataParserException e) {
            log.error("error while parsing xml contents of patent id " + patent.getId() + ", text content: "
                    + patent.getText(), e);
            return new PatentOrFault(fillRequiredDataWithNullsAndBuild(resultBuilder), e);
        }
    }

    private static Patent.Builder fillDataFromImport(Patent.Builder patentBuilder, ImportedPatent importedPatent) {
        patentBuilder.setApplnAuth(importedPatent.getApplnAuth());
        patentBuilder.setApplnNr(importedPatent.getApplnNr());
        return patentBuilder;
    }
    
    private static Patent fillRequiredDataWithNullsAndBuild(Patent.Builder patentBuilder) {
        if (!patentBuilder.hasApplicantNames()) {
            patentBuilder.setApplicantNames(null);
        }
        if (!patentBuilder.hasApplicantCountryCodes()) {
            patentBuilder.setApplicantCountryCodes(null);
        }
        if (!patentBuilder.hasApplnAbstract()) {
            patentBuilder.setApplnAbstract(null);
        }
        if (!patentBuilder.hasApplnFilingDate()) {
            patentBuilder.setApplnFilingDate(null);
        }
        if (!patentBuilder.hasApplnNrEpodoc()) {
            patentBuilder.setApplnNrEpodoc(null);
        }
        if (!patentBuilder.hasApplnTitle()) {
            patentBuilder.setApplnTitle(null);
        }
        if (!patentBuilder.hasEarliestPublnDate()) {
            patentBuilder.setEarliestPublnDate(null);
        }
        if (!patentBuilder.hasIpcClassSymbol()) {
            patentBuilder.setIpcClassSymbol(null);
        }
        return patentBuilder.build();
    }

    @Parameters(separators = "=")
    private static class JobParameters {
        @Parameter(names = "-inputImportedPatentPath", required = true)
        private String inputImportedPatentPath;
        
        @Parameter(names = "-inputDocumentTextPath", required = true)
        private String inputDocumentTextPath;
        
        @Parameter(names = "-outputPath", required = true)
        private String outputPath;
        
        @Parameter(names = "-outputFaultPath", required = true)
        private String outputFaultPath;
        
        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
    }
}
