package eu.dnetlib.iis.wf.documentsclassification;

import static eu.dnetlib.iis.wf.documentsclassification.DocumentClassificationCounters.ACM_CLASSES;
import static eu.dnetlib.iis.wf.documentsclassification.DocumentClassificationCounters.ARXIV_CLASSES;
import static eu.dnetlib.iis.wf.documentsclassification.DocumentClassificationCounters.CLASSIFIED_DOCUMENTS;
import static eu.dnetlib.iis.wf.documentsclassification.DocumentClassificationCounters.DDC_CLASSES;
import static eu.dnetlib.iis.wf.documentsclassification.DocumentClassificationCounters.MESH_EURO_PMC_CLASSES;
import static eu.dnetlib.iis.wf.documentsclassification.DocumentClassificationCounters.WOS_CLASSES;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.Accumulable;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.counter.NamedCounters;
import eu.dnetlib.iis.common.counter.NamedCountersAccumulableParam;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.report.CountersToReportEntriesConverter;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.utils.AvroGsonFactory;
import eu.dnetlib.iis.documentsclassification.schemas.DocumentClasses;
import eu.dnetlib.iis.documentsclassification.schemas.DocumentMetadata;
import eu.dnetlib.iis.documentsclassification.schemas.DocumentToDocumentClasses;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

/**
 * Document classification spark job.
 * <br/><br/>
 * Processes documents {link ExtractedDocumentMetadataMergedWithOriginal} and calculates {@link DocumentToDocumentClasses} out of them.
 * 
 * @author Łukasz Dumiszewski
 */

public class DocumentClassificationJob {
    
    private static SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static SparkAvroSaver avroSaver = new SparkAvroSaver();

    private static DocumentToDocClassificationMetadataConverter converter = new DocumentToDocClassificationMetadataConverter();
    
    private static CountersToReportEntriesConverter countersConverter = new CountersToReportEntriesConverter(DocumentClassificationCounters.getCounterNameToParamKeyMapping());
    
    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
        
        DocumentClassificationJobParameters params = new DocumentClassificationJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputAvroPath);
          
            sc.sc().addFile(params.scriptDirPath, true);
            
            
            JavaRDD<ExtractedDocumentMetadataMergedWithOriginal> documents = avroLoader.loadJavaRDD(sc, params.inputAvroPath, ExtractedDocumentMetadataMergedWithOriginal.class);
            
            JavaRDD<DocumentMetadata> metadataRecords = documents.map(document -> converter.convert(document)).filter(metadata->StringUtils.isNotBlank(metadata.getAbstract$()));
            
            
            String scriptsDirOnWorkerNode = (sc.isLocal()) ? SparkFiles.get("scripts") : "scripts";
            
            JavaRDD<String> stringDocumentClasses = metadataRecords.pipe("bash " + scriptsDirOnWorkerNode + "/classify_documents.sh" + " " + scriptsDirOnWorkerNode);
            
            
            Accumulable<NamedCounters, Tuple2<String,Long>> docClassificationJobAccumulator = sc.accumulable(
                    new NamedCounters(DocumentClassificationCounters.class), new NamedCountersAccumulableParam());
            
            JavaRDD<DocumentToDocumentClasses> documentClasses = stringDocumentClasses.map(recordString -> {
                DocumentToDocumentClasses record = AvroGsonFactory.create().fromJson(recordString, DocumentToDocumentClasses.class);
                
                updateAccumulatorCounters(docClassificationJobAccumulator, record);
                
                return record;
            });
            
            avroSaver.saveJavaRDD(documentClasses, DocumentToDocumentClasses.SCHEMA$, params.outputAvroPath);
            
            
            JavaRDD<ReportEntry> reportRDD = sc.parallelize(countersConverter.convertToReportEntries(docClassificationJobAccumulator.value()));
            avroSaver.saveJavaRDD(reportRDD, ReportEntry.SCHEMA$, params.outputReportPath);
        }
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private static void updateAccumulatorCounters(Accumulable<NamedCounters, Tuple2<String,Long>> docClassificationJobAccumulator, DocumentToDocumentClasses record) {
        
        docClassificationJobAccumulator.add(new Tuple2<>(CLASSIFIED_DOCUMENTS.name(), 1L));
        
        DocumentClasses docClasses = record.getClasses();
        docClassificationJobAccumulator.add(new Tuple2<>(ARXIV_CLASSES.name(), listSize(docClasses.getArXivClasses())));
        docClassificationJobAccumulator.add(new Tuple2<>(WOS_CLASSES.name(), listSize(docClasses.getWoSClasses())));
        docClassificationJobAccumulator.add(new Tuple2<>(DDC_CLASSES.name(), listSize(docClasses.getDDCClasses())));
        docClassificationJobAccumulator.add(new Tuple2<>(MESH_EURO_PMC_CLASSES.name(), listSize(docClasses.getMeshEuroPMCClasses())));
        docClassificationJobAccumulator.add(new Tuple2<>(ACM_CLASSES.name(), listSize(docClasses.getACMClasses())));
        
    }
    
    private static long listSize(List<?> list) {
        return (list == null) ? 0 : list.size();
    }
    
    
    @Parameters(separators = "=")
    private static class DocumentClassificationJobParameters {
        
        @Parameter(names = "-inputAvroPath", required = true)
        private String inputAvroPath;
        
        @Parameter(names = "-outputAvroPath", required = true)
        private String outputAvroPath;
        
        @Parameter(names = "-scriptDirPath", required = true, description = "path to directory with scripts")
        private String scriptDirPath;
        
        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
        
    }
}
