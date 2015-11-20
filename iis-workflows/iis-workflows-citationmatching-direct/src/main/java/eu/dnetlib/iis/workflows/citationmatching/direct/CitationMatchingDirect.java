package eu.dnetlib.iis.workflows.citationmatching.direct;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.citationmatching.direct.schemas.Citation;
import eu.dnetlib.iis.citationmatching.direct.schemas.DocumentMetadata;
import eu.dnetlib.iis.citationmatching.direct.schemas.ReferenceMetadata;
import eu.dnetlib.iis.common.spark.avro.SparkAvroLoader;
import eu.dnetlib.iis.common.spark.avro.SparkAvroSaver;
import scala.Tuple2;


public class CitationMatchingDirect {
    private static Logger log = LoggerFactory.getLogger(CitationMatchingDirect.class);
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
        
        SparkCMDirectTransformerParameters params = new SparkCMDirectTransformerParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "eu.dnetlib.iis.core.spark.AvroCompatibleKryoRegistrator");
        
        
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            
            JavaRDD<DocumentMetadata> output = SparkAvroLoader.loadJavaRDD(sc, params.inputAvroPath, DocumentMetadata.class);
//            logRDD(output, "READ DATA:");
            
            IdentifierMappingExtractor idMappingExtractor = new IdentifierMappingExtractor();
            ExternalIdReferencePicker referencePicker = new ExternalIdReferencePicker();
            
            
            JavaPairRDD<String, String> doiToId = idMappingExtractor.extractIdMapping(output, "doi", x -> x.iterator().next());
            logPairRDD(doiToId, "DOI TO ID:");
            
            
            JavaPairRDD<String, Citation> doiCitation = referencePicker.pickReferences(output, "doi");
            
            
            JavaRDD<Citation> onlyDoiCitation = doiCitation.join(doiToId)
                    .map(x -> { Citation cit = x._2._1; cit.setDestinationDocumentId(x._2._2); return cit; });
            logRDD(onlyDoiCitation, "CITATIONS FROM DOI:");
            
            
            JavaPairRDD<IdPosKey, Citation> idPosKeyDoiCitation = onlyDoiCitation.keyBy(x -> new IdPosKey((String)x.getSourceDocumentId(), x.getPosition()));
            
            
            
            
            JavaPairRDD<String, String> pmidToId = idMappingExtractor.extractIdMapping(output, "pmid", x -> {
                Iterator<DocumentMetadata> it = x.iterator();
                
                DocumentMetadata first = null;
                while(it.hasNext()) {
                    DocumentMetadata docMeta = it.next();
                    if (StringUtils.equals(docMeta.getPublicationTypeName(), "research-article")) {
                        return docMeta;
                    }
                    if (first == null) {
                        first = docMeta;
                    }
                }
                return first;
//                DocumentMetadata first = it.next();
//                return it.hasNext() ? null : first;
            });
            logPairRDD(pmidToId, "PMID TO ID:");
            
            
            JavaPairRDD<String, Citation> pmidCitation = referencePicker.pickReferences(output, "pmid");
            
            
            JavaRDD<Citation> onlyPmidCitation = pmidCitation.join(pmidToId)
                    .map(x -> { Citation cit = x._2._1; cit.setDestinationDocumentId(x._2._2); return cit; });
            logRDD(onlyPmidCitation, "CITATIONS FROM PMID:");
            
            
            JavaPairRDD<IdPosKey, Citation> idPosKeyPmidCitation = onlyPmidCitation.keyBy(x -> new IdPosKey((String)x.getSourceDocumentId(), x.getPosition()));
            
            
            
            
            JavaPairRDD<IdPosKey, Tuple2<Optional<Citation>, Optional<Citation>>> merged = idPosKeyDoiCitation.fullOuterJoin(idPosKeyPmidCitation);
            JavaRDD<Citation> citations = merged.map(x -> x._2._1.isPresent() ? x._2._1.get() : x._2._2.get() );
//            log.warn("DEBUG STRING: " + citations.toDebugString());
            logRDD(citations, "ALL CITATIONS:");
            
            SparkAvroSaver.saveJavaRDD(citations, Citation.SCHEMA$, params.outputAvroPath);
        }
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    public static class WorkingCitation {
        public String sourceId;
        public String destId;
        public String doi;
        public String pmid;
        public int position;
    }
    public static class IdPosKey {
        public String sourceId;
        public int position;
        
        public IdPosKey(String sourceId, int position) {
            this.sourceId = sourceId;
            this.position = position;
        }
        @Override
        public boolean equals(Object other) {
            if (other == null) return false;
            IdPosKey otherO = (IdPosKey)other;
            return sourceId.equals(otherO.sourceId) && position == otherO.position;
        }
        
        @Override
        public int hashCode() {
            return sourceId.hashCode() + Integer.valueOf(position).hashCode();
        }
    }
    
    private static void logPairRDD(JavaPairRDD<? extends Object, ? extends Object> pairRDD, String ... messages) {
        for (String mess: messages) {
            log.warn(mess);
        }
        log.warn(pairRDD.collect().stream().map(x -> "(" + x._1 + "|" + x._2 + ")").collect(Collectors.joining(", ")));
    }
    
    private static void logRDD(JavaRDD<? extends Object> rdd, String ... messages) {
        for (String mess: messages) {
            log.warn(mess);
        }
        log.warn(rdd.collect().stream().map(x -> x.toString()).collect(Collectors.joining(", ")));
    }
    
    @Parameters(separators = "=")
    private static class SparkCMDirectTransformerParameters {
        
        @Parameter(names = "-inputAvroPath", required = true)
        private String inputAvroPath;
        
        @Parameter(names = "-outputAvroPath", required = true)
        private String outputAvroPath;
        
        
    }
}
