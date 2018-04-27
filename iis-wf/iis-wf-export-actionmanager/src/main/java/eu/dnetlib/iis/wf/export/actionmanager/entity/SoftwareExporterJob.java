package eu.dnetlib.iis.wf.export.actionmanager.entity;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.actionmanager.actions.ActionFactory;
import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.data.mapreduce.util.OafDecoder;
import eu.dnetlib.data.proto.FieldTypeProtos.KeyValue;
import eu.dnetlib.data.proto.FieldTypeProtos.Qualifier;
import eu.dnetlib.data.proto.FieldTypeProtos.StringField;
import eu.dnetlib.data.proto.FieldTypeProtos.StructuredProperty;
import eu.dnetlib.data.proto.KindProtos.Kind;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.data.proto.OafProtos.OafRel;
import eu.dnetlib.data.proto.RelTypeProtos.RelType;
import eu.dnetlib.data.proto.RelTypeProtos.SubRelType;
import eu.dnetlib.data.proto.ResultProtos.Result;
import eu.dnetlib.data.proto.ResultProtos.Result.Instance;
import eu.dnetlib.data.proto.ResultProtos.Result.Metadata;
import eu.dnetlib.data.proto.ResultResultProtos.ResultResult;
import eu.dnetlib.data.proto.ResultResultProtos.ResultResult.Relationship;
import eu.dnetlib.data.proto.TypeProtos.Type;
import eu.dnetlib.data.transform.xml.AbstractDNetXsltFunctions;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.DocumentToSoftwareUrlWithMeta;
import eu.dnetlib.iis.wf.export.actionmanager.cfg.StaticConfigurationProvider;
import eu.dnetlib.iis.wf.export.actionmanager.module.AlgorithmName;
import eu.dnetlib.iis.wf.export.actionmanager.module.BuilderModuleHelper;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import scala.Tuple2;
import scala.Tuple3;

/**
 * Software entity and relations exporter reading {@link DocumentToSoftwareUrlWithMeta} avro records and exporting them as entity and relation actions.
 * 
 * @author mhorst
 *
 */
public class SoftwareExporterJob {
    
    public static final String REL_CLASS_ISRELATEDTO = Relationship.RelName.isRelatedTo.toString();
    
    public static final String INFERENCE_PROVENANCE = InfoSpaceConstants.SEMANTIC_CLASS_IIS
            + InfoSpaceConstants.INFERENCE_PROVENANCE_SEPARATOR + AlgorithmName.document_software_url;
    
    private static final Qualifier RESULT_TYPE_SOFTWARE = buildResultTypeSoftware();
    
    private static final Qualifier INSTANCE_TYPE_SOFTWARE = buildInstanceTypeSoftware();
    
    private static final Qualifier ACCESS_RIGHT_UNKNOWN = buildAccessRightUnknown();
    
    private static final String OPENAIRE_ENTITY_ID_PREFIX = "openaire____::";
    
    private static final int numberOfOutputFiles = 10;
    
    private static final DateFormat dateFormat = buildDateFormat();

    private static SparkAvroLoader avroLoader = new SparkAvroLoader();
    
    private static SoftwareExportCounterReporter counterReporter = new SoftwareExportCounterReporter();
    
    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws Exception {
        
        SoftwareEntityExporterJobParameters params = new SoftwareEntityExporterJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputEntityPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputRelationPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputReportPath);
            
            final Float confidenceLevelThreshold = (StringUtils.isNotBlank(params.trustLevelThreshold)
                    && !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(params.trustLevelThreshold))
                            ? Float.valueOf(params.trustLevelThreshold) / InfoSpaceConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR : null; 
            
            JavaRDD<DocumentToSoftwareUrlWithMeta> documentToSoftwareUrlWithMetaFiltered = avroLoader.loadJavaRDD(sc, params.inputAvroPath,
                    DocumentToSoftwareUrlWithMeta.class).filter(f -> (isValidEntity(f) && isValidConfidenceLevel(f, confidenceLevelThreshold)));
            // to be used by both entity and relation processing paths
            documentToSoftwareUrlWithMetaFiltered.cache();
            
            Job job = Job.getInstance();
            job.getConfiguration().set(FileOutputFormat.COMPRESS, Boolean.TRUE.toString());
            job.getConfiguration().set(FileOutputFormat.COMPRESS_TYPE, CompressionType.BLOCK.name());

            JavaRDD<DocumentToSoftwareUrlWithMeta> dedupedDocumentToSoftware = handleEntities(documentToSoftwareUrlWithMetaFiltered, 
                    params.entityActionSetId, job.getConfiguration(), params.outputEntityPath);

            JavaRDD<Tuple3<String, String, Float>> dedupedRelationTriples = handleRelations(documentToSoftwareUrlWithMetaFiltered, 
                    params.relationActionSetId, job.getConfiguration(), params.outputRelationPath);
            
            // reporting
            counterReporter.report(sc, dedupedDocumentToSoftware, dedupedRelationTriples, params.outputReportPath);
        }
    }
    
    
    // ----------------------------------------- PRIVATE ----------------------------------------------
    
    private static JavaRDD<DocumentToSoftwareUrlWithMeta> handleEntities(JavaRDD<DocumentToSoftwareUrlWithMeta> documentToSoftwareUrl, String actionSetId, 
            Configuration jobConfig, String outputAvroPath) {

        JavaPairRDD<String, DocumentToSoftwareUrlWithMeta> documentToSoftwareByUrl = documentToSoftwareUrl.mapToPair(e -> new Tuple2<>(pickUrl(e), e)); 
        
        JavaPairRDD<String, DocumentToSoftwareUrlWithMeta> documentToSoftwareReduced = documentToSoftwareByUrl.reduceByKey((x, y) -> pickBestFilled(x,y));
        
        JavaRDD<DocumentToSoftwareUrlWithMeta> documentToSoftwareReducedValues = documentToSoftwareReduced.values();
        // to be used by both entity exporter and reporter consumers
        documentToSoftwareReducedValues.cache();
        
        JavaPairRDD<Text, Text> entityResult = documentToSoftwareReducedValues.map(e -> buildEntityAction(e, actionSetId)).mapToPair(
                action -> new Tuple2<Text, Text>(new Text(action.getRowKey()), new Text(action.toString())));
        entityResult.coalesce(numberOfOutputFiles).saveAsNewAPIHadoopFile(outputAvroPath, Text.class, Text.class, SequenceFileOutputFormat.class, jobConfig);
        
        return documentToSoftwareReducedValues;
    }
    
    private static JavaRDD<Tuple3<String, String, Float>> handleRelations(JavaRDD<DocumentToSoftwareUrlWithMeta> documentToSoftwareUrl, String actionSetId, 
            Configuration jobConfig, String outputAvroPath) {
        JavaRDD<Tuple3<String, String, Float>> distinctRelationTriples = documentToSoftwareUrl
                .map(e -> new Tuple3<>(e.getDocumentId().toString(), generateSoftwareEntityId(pickUrl(e)), e.getConfidenceLevel()))
                .distinct();
        
        JavaPairRDD<String, Tuple3<String, String, Float>> relationTriplesByIdPair = distinctRelationTriples
                .mapToPair(e -> new Tuple2<String, Tuple3<String, String, Float>>(
                        joinDocumentAndSoftwareIds(e._1(), e._2()), e));
        
        JavaRDD<Tuple3<String, String, Float>> dedupedRelationTriples = relationTriplesByIdPair
                .reduceByKey((x, y) -> pickBestConfidence(x, y)).values();
        // to be used by both entity exporter and reporter consumers
        dedupedRelationTriples.cache();
        
        JavaPairRDD<Text, Text> relationResult = dedupedRelationTriples.flatMapToPair(x -> (Iterable<Tuple2<Text, Text>>) 
                buildRelationActions(x._1(), x._2(), x._3(), actionSetId).stream()
                .map(action -> new Tuple2<Text, Text>(new Text(action.getRowKey()),
                        new Text(action.toString())))::iterator);
        relationResult.coalesce(numberOfOutputFiles).saveAsNewAPIHadoopFile(outputAvroPath, Text.class, Text.class, SequenceFileOutputFormat.class, jobConfig);
        
        return dedupedRelationTriples;
    }
    
    private static String joinDocumentAndSoftwareIds(CharSequence documentId, String softwareId) {
        StringBuffer strBuff = new StringBuffer(documentId);
        strBuff.append("#");
        strBuff.append(softwareId);
        return strBuff.toString();
    }
    
    private static Tuple3<String, String, Float> pickBestConfidence(
            Tuple3<String, String, Float> triple1, Tuple3<String, String, Float> triple2) {
        return triple1._3() >= triple2._3() ? triple1 : triple2;
    }

    private static DocumentToSoftwareUrlWithMeta pickBestFilled(DocumentToSoftwareUrlWithMeta meta1, DocumentToSoftwareUrlWithMeta meta2) {
        if (StringUtils.isNotBlank(meta1.getSoftwarePageURL())) {
            return meta1;
        } else {
            if (StringUtils.isNotBlank(meta2.getSoftwarePageURL())) {
                return meta2;
            } else {
                // pageURL was missing in both, picking the one with description set
                if (StringUtils.isNotBlank(meta1.getSoftwareDescription())) {
                    return meta1;
                } else {
                    return meta2;
                }
            }
        }
    }
    
    protected static String generateSoftwareEntityId(String url) {
        return InfoSpaceConstants.ROW_PREFIX_RESULT + OPENAIRE_ENTITY_ID_PREFIX + AbstractDNetXsltFunctions.md5(url);
    }
    
    private static String generateDatasourceId(String repositoryName) {
        return InfoSpaceConstants.ROW_PREFIX_DATASOURCE + OPENAIRE_ENTITY_ID_PREFIX + AbstractDNetXsltFunctions.md5(repositoryName);
    }
    
    private static AtomicAction buildEntityAction(DocumentToSoftwareUrlWithMeta object, String actionSetId) {
            Oaf softwareOaf = buildSoftwareOaf(object);
            OafDecoder oafDecoder = OafDecoder.decode(softwareOaf);
            return getActionFactory().createAtomicAction(actionSetId, StaticConfigurationProvider.AGENT_DEFAULT, 
                    oafDecoder.getEntityId(), oafDecoder.getCFQ(), 
                    InfoSpaceConstants.QUALIFIER_BODY_STRING, softwareOaf.toByteArray());
    }
    
    private static Oaf buildSoftwareOaf(DocumentToSoftwareUrlWithMeta object) {
        
        String url = pickUrl(object);
        
        OafEntity.Builder entityBuilder = OafEntity.newBuilder();
        entityBuilder.setId(generateSoftwareEntityId(url));
        entityBuilder.setType(Type.result);
        
        String dateStr = dateFormat.format(new Date());
        entityBuilder.setDateofcollection(dateStr);
        entityBuilder.setDateoftransformation(dateStr);
        
        Result.Builder resultBuilder = Result.newBuilder();
        
        Metadata.Builder metaBuilder = Metadata.newBuilder();
        metaBuilder.setCodeRepositoryUrl(StringField.newBuilder().setValue(url).build());

        if (StringUtils.isNotBlank(object.getSoftwareTitle())) {
            metaBuilder.addTitle(buildTitle(object.getSoftwareTitle().toString()));
        }
        
        if (StringUtils.isNotBlank(object.getSoftwareDescription())) {
            metaBuilder.addDescription(
                    StringField.newBuilder().setValue(object.getSoftwareDescription().toString()).build());
        }
        
        metaBuilder.setResulttype(RESULT_TYPE_SOFTWARE);
        
        resultBuilder.setMetadata(metaBuilder.build());
        
        Instance.Builder instanceBuilder = Instance.newBuilder();
        instanceBuilder.addUrl(url);
        instanceBuilder.setInstancetype(INSTANCE_TYPE_SOFTWARE);
        instanceBuilder.setAccessright(ACCESS_RIGHT_UNKNOWN);
        
        if (StringUtils.isNotBlank(object.getRepositoryName())) {
            KeyValue collectedFrom = buildHostedBy(object.getRepositoryName().toString());
            instanceBuilder.setHostedby(collectedFrom);
            instanceBuilder.setCollectedfrom(collectedFrom);
            entityBuilder.addCollectedfrom(collectedFrom);
        }
        
        resultBuilder.addInstance(instanceBuilder.build());
        
        entityBuilder.setResult(resultBuilder.build());
        return BuilderModuleHelper.buildOaf(entityBuilder.build(), object.getConfidenceLevel(), INFERENCE_PROVENANCE);
    }
    
    private static DateFormat buildDateFormat() {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        return df;
    }
    
    private static StructuredProperty buildTitle(String title) {
        StructuredProperty.Builder titleBuilder = StructuredProperty.newBuilder();
        titleBuilder.setValue(title);
        
        Qualifier.Builder qualifierBuilder = Qualifier.newBuilder();
        qualifierBuilder.setClassid(InfoSpaceConstants.SEMANTIC_CLASS_MAIN_TITLE);
        qualifierBuilder.setClassname(InfoSpaceConstants.SEMANTIC_CLASS_MAIN_TITLE);
        qualifierBuilder.setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_TITLE);
        qualifierBuilder.setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_TITLE);
        titleBuilder.setQualifier(qualifierBuilder.build());
        
        return titleBuilder.build();
    }
    
    private static Qualifier buildResultTypeSoftware() {
        Qualifier.Builder qualifierBuilder = Qualifier.newBuilder();
        qualifierBuilder.setClassid(InfoSpaceConstants.SEMANTIC_CLASS_SOFTWARE);
        qualifierBuilder.setClassname(InfoSpaceConstants.SEMANTIC_CLASS_SOFTWARE);
        qualifierBuilder.setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_RESULT_TYPOLOGIES);
        qualifierBuilder.setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_RESULT_TYPOLOGIES);
        return qualifierBuilder.build();
    }
    
    private static Qualifier buildInstanceTypeSoftware() {
        Qualifier.Builder qualifierBuilder = Qualifier.newBuilder();
        qualifierBuilder.setClassid(InfoSpaceConstants.SEMANTIC_CLASS_INSTANCE_TYPE_SOFTWARE);
        qualifierBuilder.setClassname(InfoSpaceConstants.SEMANTIC_CLASS_PUBLICATION_RESOURCE_SOFTWARE);
        qualifierBuilder.setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PUBLICATION_RESOURCE);
        qualifierBuilder.setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PUBLICATION_RESOURCE);
        return qualifierBuilder.build();
    }
    
    private static Qualifier buildAccessRightUnknown() {
        Qualifier.Builder qualifierBuilder = Qualifier.newBuilder();
        qualifierBuilder.setClassid(InfoSpaceConstants.SEMANTIC_CLASS_OPEN_SOURCE);
        qualifierBuilder.setClassname(InfoSpaceConstants.SEMANTIC_CLASS_NAME_OPEN_SOURCE);
        qualifierBuilder.setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_ACCESS_MODES);
        qualifierBuilder.setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_ACCESS_MODES);
        return qualifierBuilder.build();
    }
    
    private static KeyValue buildHostedBy(String repositoryName) {
        KeyValue.Builder hostedByBuilder = KeyValue.newBuilder();
        hostedByBuilder.setKey(generateDatasourceId(repositoryName));
        hostedByBuilder.setValue(repositoryName);
        return hostedByBuilder.build();
    }
    
    private static List<AtomicAction> buildRelationActions(
            String docId, String softId, Float confidenceLevel, String actionSetId) {
        Oaf.Builder oafBuilder = instantiateOafBuilderForRelation(docId, softId, confidenceLevel);
        Oaf oaf = oafBuilder.build();
        Oaf oafInverted = BuilderModuleHelper.invertBidirectionalRelationAndBuild(oafBuilder);
        return Arrays.asList(new AtomicAction[] {
                getActionFactory().createAtomicAction(actionSetId, StaticConfigurationProvider.AGENT_DEFAULT, docId, 
                        OafDecoder.decode(oaf).getCFQ(), softId, oaf.toByteArray()),
                // setting reverse relation in referenced object
                getActionFactory().createAtomicAction(actionSetId, StaticConfigurationProvider.AGENT_DEFAULT, softId, 
                        OafDecoder.decode(oafInverted).getCFQ(), docId, oafInverted.toByteArray()) });
    }
    
    private static Oaf.Builder instantiateOafBuilderForRelation(String docId, String softId, Float confidenceLevel) {
        Oaf.Builder oafBuilder = Oaf.newBuilder();
        oafBuilder.setKind(Kind.relation);
        oafBuilder.setRel(buildOafRel(docId, softId));
        oafBuilder.setDataInfo(
                BuilderModuleHelper.buildInferenceForConfidenceLevel(confidenceLevel, INFERENCE_PROVENANCE));
        oafBuilder.setLastupdatetimestamp(System.currentTimeMillis());
        return oafBuilder;
    }
    
    private static OafRel buildOafRel(String docId, String softId) {
        OafRel.Builder relBuilder = OafRel.newBuilder();
        relBuilder.setChild(false);
        relBuilder.setRelType(RelType.resultResult);
        relBuilder.setSubRelType(SubRelType.relationship);
        relBuilder.setRelClass(REL_CLASS_ISRELATEDTO);
        relBuilder.setSource(docId);
        relBuilder.setTarget(softId);
        ResultResult.Builder resResultBuilder = ResultResult.newBuilder();
        Relationship.Builder relationshipBuilder = Relationship.newBuilder();
        relationshipBuilder.setRelMetadata(BuilderModuleHelper.buildRelMetadata(
                InfoSpaceConstants.SEMANTIC_SCHEME_DNET_RELATIONS_RESULT_RESULT, REL_CLASS_ISRELATEDTO));
        resResultBuilder.setRelationship(relationshipBuilder.build());
        relBuilder.setResultResult(resResultBuilder.build());
        return relBuilder.build();
    }
    
    private static boolean isValidEntity(DocumentToSoftwareUrlWithMeta source) {
        return (source != null && StringUtils.isNotBlank(source.getSoftwareTitle()) &&
                (StringUtils.isNotBlank(source.getSoftwarePageURL()) || StringUtils.isNotBlank(source.getSoftwareUrl())));
    }
    
    private static boolean isValidConfidenceLevel(DocumentToSoftwareUrlWithMeta source, Float confidenceLevelThreshold) {
        return confidenceLevelThreshold == null || source.getConfidenceLevel() >= confidenceLevelThreshold;
    }
    
    private static String pickUrl(DocumentToSoftwareUrlWithMeta source) {
        return StringUtils.isNotBlank(source.getSoftwarePageURL()) ? source.getSoftwarePageURL().toString()
                : source.getSoftwareUrl().toString();
    }
    
    private static ActionFactory getActionFactory() {
        return new ActionFactory();
    }
    
    @Parameters(separators = "=")
    private static class SoftwareEntityExporterJobParameters {
        
        @Parameter(names = "-inputAvroPath", required = true)
        private String inputAvroPath;
        
        @Parameter(names = "-outputEntityPath", required = true)
        private String outputEntityPath;
        
        @Parameter(names = "-outputRelationPath", required = true)
        private String outputRelationPath;
        
        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
        
        @Parameter(names = "-entityActionSetId", required = true)
        private String entityActionSetId;
        
        @Parameter(names = "-relationActionSetId", required = true)
        private String relationActionSetId;
        
        @Parameter(names = "-trustLevelThreshold", required = false)
        private String trustLevelThreshold;

    }
    
}
