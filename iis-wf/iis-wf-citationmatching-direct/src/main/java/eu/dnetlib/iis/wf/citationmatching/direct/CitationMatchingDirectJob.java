package eu.dnetlib.iis.wf.citationmatching.direct;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.citationmatching.direct.schemas.Citation;
import eu.dnetlib.iis.citationmatching.direct.schemas.DocumentMetadata;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import eu.dnetlib.iis.wf.citationmatching.direct.converter.DirectCitationToCitationConverter;
import eu.dnetlib.iis.wf.citationmatching.direct.converter.DocumentToDirectCitationMetadataConverter;
import eu.dnetlib.iis.wf.citationmatching.direct.model.IdWithPosition;
import eu.dnetlib.iis.wf.citationmatching.direct.service.CitationMatchingDirectCounterReporter;
import eu.dnetlib.iis.wf.citationmatching.direct.service.ExternalIdCitationMatcher;
import eu.dnetlib.iis.wf.citationmatching.direct.service.PickFirstDocumentFunction;
import eu.dnetlib.iis.wf.citationmatching.direct.service.PickResearchArticleDocumentFunction;


public class CitationMatchingDirectJob {
    
    private static final String CSV_COLUMN_PMCID = "PMCID";
    private static final String CSV_COLUMN_PMID = "PMID";
    
    private static final String EXTID_DOI = "doi";
    private static final String EXTID_PMID = "pmid";
    private static final String EXTID_PMC = "pmc";
    
    private static SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static SparkAvroSaver avroSaver = new SparkAvroSaver();
    
    private static DocumentToDirectCitationMetadataConverter documentToDirectCitationMetadataConverter = new DocumentToDirectCitationMetadataConverter();
    
    private static ExternalIdCitationMatcher externalIdCitationMatcher = new ExternalIdCitationMatcher();
    
    private static DirectCitationToCitationConverter directCitationToCitationConverter = new DirectCitationToCitationConverter();
    
    private static CitationMatchingDirectCounterReporter citationMatchingDirectReporter = new CitationMatchingDirectCounterReporter(
            "processing.citationMatching.direct.doc", "processing.citationMatching.direct.citDocReference");
    
    
    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
        
        CitationMatchingDirectJobParameters params = new CitationMatchingDirectJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        
        
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputAvroPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputReportPath);
            
            JavaRDD<ExtractedDocumentMetadataMergedWithOriginal> documents = avroLoader.loadJavaRDD(sc, params.inputAvroPath, ExtractedDocumentMetadataMergedWithOriginal.class);
            
            
            JavaRDD<DocumentMetadata> simplifiedDocuments = documents.map(document -> documentToDirectCitationMetadataConverter.convert(document));
            simplifiedDocuments = simplifiedDocuments.cache();
            
            JavaRDD<Citation> doiCitations = externalIdCitationMatcher.matchCitations(simplifiedDocuments, EXTID_DOI, new PickFirstDocumentFunction());
            
            JavaRDD<Citation> pmidCitations = externalIdCitationMatcher.matchCitations(simplifiedDocuments, EXTID_PMID, new PickResearchArticleDocumentFunction());
            
            JavaRDD<Citation> mergedCitations;
            
            if (StringUtils.isNotBlank(params.inputPmcIdsMappingCSV) && !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(params.inputPmcIdsMappingCSV)) {
                JavaRDD<Citation> pmcidByPmidCitations = externalIdCitationMatcher.matchCitations(simplifiedDocuments, 
                        loadPmidToPmcidMappings(sc, params.inputPmcIdsMappingCSV), EXTID_PMC, EXTID_PMID, new PickResearchArticleDocumentFunction());
                
                mergedCitations = mergeCitations(mergeCitations(doiCitations, pmidCitations), pmcidByPmidCitations);
            } else {
                mergedCitations = mergeCitations(doiCitations, pmidCitations);
            }
            
            JavaRDD<eu.dnetlib.iis.common.citations.schemas.Citation> citations = 
                    mergedCitations.map(directCitation -> directCitationToCitationConverter.convert(directCitation));
            
            citations.cache();
            citationMatchingDirectReporter.report(sc, citations, params.outputReportPath);
            
            avroSaver.saveJavaRDD(citations, eu.dnetlib.iis.common.citations.schemas.Citation.SCHEMA$, params.outputAvroPath);
        }
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    /**
     * Loads pmid and pmcid identifier pairs obtained from CSV file.
     * @param sc spark context required to build SQL context
     * @param inputCSV HDFS CSV file location
     */
    private static JavaPairRDD<String, String> loadPmidToPmcidMappings(JavaSparkContext sc, String inputCSV) {
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame pmcDF = sqlContext.read().format("com.databricks.spark.csv").option("header", "true").load(inputCSV);
        DataFrame pmcSelectedDF = pmcDF.select(pmcDF.col(CSV_COLUMN_PMID),pmcDF.col(CSV_COLUMN_PMCID));
        DataFrame pmcFilteredDF = pmcSelectedDF.filter(pmcSelectedDF.col(CSV_COLUMN_PMID).isNotNull().and(pmcSelectedDF.col(CSV_COLUMN_PMCID).isNotNull()));
        return pmcFilteredDF.toJavaRDD().mapToPair(r -> new Tuple2<>(r.getString(0), r.getString(1)));
    }
    
    private static JavaRDD<Citation> mergeCitations(JavaRDD<Citation> existingCitations, JavaRDD<Citation> newCitations) {
        
        JavaPairRDD<IdWithPosition, Citation> existingCitationsWithKey = attachIdWithPositionKey(existingCitations);
        JavaPairRDD<IdWithPosition, Citation> newCitationsWithKey = attachIdWithPositionKey(newCitations);
        
        JavaRDD<Citation> mergedCitations = existingCitationsWithKey.fullOuterJoin(newCitationsWithKey)
                .map(x -> x._2._1.isPresent() ? x._2._1.get() : x._2._2.get() );
        
        return mergedCitations;
    }
    
    private static JavaPairRDD<IdWithPosition, Citation> attachIdWithPositionKey(JavaRDD<Citation> directCitations) {
        JavaPairRDD<IdWithPosition, Citation> directCitationsWithKey = directCitations
                .keyBy(directCitation -> new IdWithPosition(directCitation.getSourceDocumentId().toString(), directCitation.getPosition()));
        
        return directCitationsWithKey;
    }
    
    
    @Parameters(separators = "=")
    private static class CitationMatchingDirectJobParameters {
        
        @Parameter(names = "-inputAvroPath", required = true)
        private String inputAvroPath;
        
        /**
         * CSV dump HDFS location. Contains entries with mappings between different kinds of PMC identifiers for each publication. 
         * More details on:
         * https://www.ncbi.nlm.nih.gov/pmc/pmctopmid#ftp
         */
        @Parameter(names = "-inputPmcIdsMappingCSV", required = true)
        private String inputPmcIdsMappingCSV;
        
        @Parameter(names = "-outputAvroPath", required = true)
        private String outputAvroPath;
        
        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
    }
}
