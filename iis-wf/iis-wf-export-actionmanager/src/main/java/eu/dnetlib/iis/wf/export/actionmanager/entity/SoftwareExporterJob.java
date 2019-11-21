package eu.dnetlib.iis.wf.export.actionmanager.entity;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
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
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.utils.DateTimeUtils;
import eu.dnetlib.iis.common.utils.RDDUtils;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.DocumentToSoftwareUrlWithMeta;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import eu.dnetlib.iis.wf.export.actionmanager.cfg.StaticConfigurationProvider;
import eu.dnetlib.iis.wf.export.actionmanager.module.AlgorithmName;
import eu.dnetlib.iis.wf.export.actionmanager.module.BuilderModuleHelper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import scala.Tuple2;
import scala.Tuple3;

import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
    
    private static final Qualifier ACCESS_RIGHT_OPEN_SOURCE = buildAccessRightOpenSource();

    private static final String COLLECTED_FROM_SOFTWARE_HERITAGE_BASE_ID = "SoftwareHeritage";
    
    private static final String COLLECTED_FROM_SOFTWARE_HERITAGE_NAME = "Software Heritage";
    
    private static final String SOFTWARE_REPOSITORY_OTHER = "Other";
    
    private static final String SOFTWARE_TITLE_MAIN_TEMPLATE = "{0} software on {1}";
    
    private static final String SOFTWARE_TITLE_ALTERNATIVE_TEMPLATE = "{0} software on {1} in support of ''{2}''";
    
    private static final int numberOfOutputFiles = 10;
    
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

            final Float confidenceLevelThreshold = ConfidenceLevelUtils
                    .evaluateConfidenceLevelThreshold(params.trustLevelThreshold);

            JavaRDD<ExtractedDocumentMetadataMergedWithOriginal> documentMetadata = avroLoader.loadJavaRDD(sc,
                    params.inputDocumentMetadataAvroPath, ExtractedDocumentMetadataMergedWithOriginal.class);
            
            JavaPairRDD<CharSequence, CharSequence> documentIdToTitle = documentMetadata
                    .filter(e -> StringUtils.isNotBlank(e.getTitle()))
                    .mapToPair(e -> new Tuple2<>(e.getId(), e.getTitle()));

            JavaRDD<DocumentToSoftwareUrlWithMeta> documentToSoftwareUrlWithMetaFiltered = avroLoader
                    .loadJavaRDD(sc, params.inputDocumentToSoftwareAvroPath, DocumentToSoftwareUrlWithMeta.class)
                    .filter(f -> (isValidEntity(f) && ConfidenceLevelUtils.isValidConfidenceLevel(f.getConfidenceLevel(), confidenceLevelThreshold)));
            // to be used by both entity and relation processing paths
            documentToSoftwareUrlWithMetaFiltered.cache();
            
            Job job = Job.getInstance();
            job.getConfiguration().set(FileOutputFormat.COMPRESS, Boolean.TRUE.toString());
            job.getConfiguration().set(FileOutputFormat.COMPRESS_TYPE, CompressionType.BLOCK.name());

            JavaRDD<?> dedupedSoftwareUrls = handleEntities(documentToSoftwareUrlWithMetaFiltered, 
                    documentIdToTitle, params.entityActionSetId, job.getConfiguration(), params.outputEntityPath);

            JavaRDD<Tuple3<String, String, Float>> dedupedRelationTriples = handleRelations(documentToSoftwareUrlWithMetaFiltered, 
                    params.relationActionSetId, job.getConfiguration(), params.outputRelationPath);
            
            // reporting
            counterReporter.report(sc, dedupedSoftwareUrls, dedupedRelationTriples, params.outputReportPath);
        }
    }
    
    
    // ----------------------------------------- PRIVATE ----------------------------------------------
    
    private static JavaRDD<?> handleEntities(JavaRDD<DocumentToSoftwareUrlWithMeta> documentToSoftwareUrl, 
            JavaPairRDD<CharSequence, CharSequence> documentIdToTitle, 
            String actionSetId, Configuration jobConfig, String outputAvroPath) {
        
        // supplementing empty software titles with templates based on document->software relations
        JavaPairRDD<CharSequence, DocumentToSoftwareUrlWithMeta> documentToSoftwareByPublicationId = documentToSoftwareUrl.mapToPair(e -> new Tuple2<>(e.getDocumentId(), e));
        JavaPairRDD<CharSequence, Tuple2<DocumentToSoftwareUrlWithMeta, Optional<CharSequence>>> documentToSoftwareByPublicationIdJoined = documentToSoftwareByPublicationId.leftOuterJoin(documentIdToTitle);
        JavaRDD<Tuple2<DocumentToSoftwareUrlWithMeta, Set<CharSequence>>> documentToSoftwareUrlWithPubTitles = documentToSoftwareByPublicationIdJoined.map(e -> convertTitleToSetOfTitles(e._2._1, e._2._2));
        
        JavaPairRDD<String, Tuple2<DocumentToSoftwareUrlWithMeta, Set<CharSequence>>> documentToSoftwareWithPubTitlesByUrl = documentToSoftwareUrlWithPubTitles.mapToPair(e -> new Tuple2<>(pickUrl(e._1), e)); 

        JavaPairRDD<String, Tuple2<DocumentToSoftwareUrlWithMeta, Set<CharSequence>>> documentToSoftwareWithPubTitlesReduced = documentToSoftwareWithPubTitlesByUrl.reduceByKey((x, y) -> mergeSoftwareRecords(x,y));
        
        JavaRDD<Tuple2<DocumentToSoftwareUrlWithMeta, Set<CharSequence>>> documentToSoftwareReducedValues = documentToSoftwareWithPubTitlesReduced.values();
        // to be used by both entity exporter and reporter consumers
        documentToSoftwareReducedValues.cache();
        
        JavaPairRDD<Text, Text> entityResult = documentToSoftwareReducedValues
                .map(e -> buildEntityAction(e, actionSetId))
                .mapToPair(action -> new Tuple2<>(new Text(action.getRowKey()), new Text(action.toString())));

        RDDUtils.saveTextPairRDD(entityResult, numberOfOutputFiles, outputAvroPath, jobConfig);

        return documentToSoftwareReducedValues;
    }
    
    private static Tuple2<DocumentToSoftwareUrlWithMeta, Set<CharSequence>> convertTitleToSetOfTitles(DocumentToSoftwareUrlWithMeta meta, Optional<CharSequence> referencedPubTitle) {
        return new Tuple2<>(meta,
                referencedPubTitle.isPresent() && StringUtils.isNotBlank(referencedPubTitle.get())
                        ? Sets.<CharSequence>newHashSet(referencedPubTitle.get())
                        : Sets.<CharSequence>newHashSet());
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

        JavaPairRDD<Text, Text> relationResult = dedupedRelationTriples
                .flatMapToPair(x ->
                        buildRelationActions(x._1(), x._2(), x._3(), actionSetId).stream().map(action -> new Tuple2<>(new Text(action.getRowKey()), new Text(action.toString())))::iterator);

        RDDUtils.saveTextPairRDD(relationResult, numberOfOutputFiles, outputAvroPath, jobConfig);

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

    private static Tuple2<DocumentToSoftwareUrlWithMeta, Set<CharSequence>> mergeSoftwareRecords(Tuple2<DocumentToSoftwareUrlWithMeta, Set<CharSequence>> tuple1, 
            Tuple2<DocumentToSoftwareUrlWithMeta, Set<CharSequence>> tuple2) {
        // assuming DocumentToSoftwareUrlWithMeta holds the same set of software related fields in both tuples (since the url was the same)
        return new Tuple2<>(tuple1._1, mergeTitles(tuple1._2(), tuple2._2()));
    }
    
    private static Set<CharSequence> mergeTitles(Set<CharSequence> sourceTitles1, Set<CharSequence> sourceTitles2) {
        if (CollectionUtils.isNotEmpty(sourceTitles1)) {
            if (CollectionUtils.isNotEmpty(sourceTitles2)) {
                Set<CharSequence> distinct = new HashSet<>();
                distinct.addAll(sourceTitles1);
                distinct.addAll(sourceTitles2);
                return distinct;
            } else {
                return sourceTitles1;
            }
        } else {
            return sourceTitles2;
        }
    }

    protected static String generateSoftwareEntityId(String url) {
        return InfoSpaceConstants.ROW_PREFIX_RESULT + InfoSpaceConstants.OPENAIRE_ENTITY_ID_PREFIX +
                InfoSpaceConstants.ID_NAMESPACE_SEPARATOR + AbstractDNetXsltFunctions.md5(url);
    }

    private static String generateDatasourceId(String repositoryName) {
        return InfoSpaceConstants.ROW_PREFIX_DATASOURCE + InfoSpaceConstants.OPENAIRE_ENTITY_ID_PREFIX +
                InfoSpaceConstants.ID_NAMESPACE_SEPARATOR + AbstractDNetXsltFunctions.md5(repositoryName);
    }

    private static AtomicAction buildEntityAction(Tuple2<DocumentToSoftwareUrlWithMeta, Set<CharSequence>> object, String actionSetId) {
            Oaf softwareOaf = buildSoftwareOaf(object);
            OafDecoder oafDecoder = OafDecoder.decode(softwareOaf);
            return getActionFactory().createAtomicAction(actionSetId, StaticConfigurationProvider.AGENT_DEFAULT, 
                    oafDecoder.getEntityId(), oafDecoder.getCFQ(), 
                    InfoSpaceConstants.QUALIFIER_BODY_STRING, softwareOaf.toByteArray());
    }
    
    private static Oaf buildSoftwareOaf(Tuple2<DocumentToSoftwareUrlWithMeta, Set<CharSequence>> metaWithPublicationTitles) {
        
        DocumentToSoftwareUrlWithMeta meta = metaWithPublicationTitles._1;
        
        String url = pickUrl(meta);
        
        OafEntity.Builder entityBuilder = OafEntity.newBuilder();
        entityBuilder.setId(generateSoftwareEntityId(url));
        entityBuilder.setType(Type.result);

        String dateStr = DateTimeUtils.format(LocalDateTime.now());
        entityBuilder.setDateofcollection(dateStr);
        entityBuilder.setDateoftransformation(dateStr);
        
        Result.Builder resultBuilder = Result.newBuilder();
        
        Metadata.Builder metaBuilder = Metadata.newBuilder();
        metaBuilder.setCodeRepositoryUrl(StringField.newBuilder().setValue(url).build());

        metaBuilder.addTitle(buildMainTitle(meta.getSoftwareTitle().toString(), meta.getRepositoryName().toString()));

        for (CharSequence currentPubTitle : metaWithPublicationTitles._2) {
            metaBuilder.addTitle(buildContextualisedTitle(meta.getSoftwareTitle().toString(),
                    meta.getRepositoryName().toString(), currentPubTitle.toString()));
        }
        
        if (StringUtils.isNotBlank(meta.getSoftwareDescription())) {
            metaBuilder.addDescription(
                    StringField.newBuilder().setValue(meta.getSoftwareDescription().toString()).build());
        }
        
        metaBuilder.setResulttype(RESULT_TYPE_SOFTWARE);

        resultBuilder.setMetadata(metaBuilder.build());
        
        if (StringUtils.isNotBlank(meta.getSHUrl())) {
            //Software Heritage mode
            KeyValue collectedFromSH = buildHostedBy(generateDatasourceId(COLLECTED_FROM_SOFTWARE_HERITAGE_BASE_ID),
                    COLLECTED_FROM_SOFTWARE_HERITAGE_NAME);
            KeyValue collectedFromOrigin = buildHostedBy(meta.getRepositoryName().toString());
            
            //origin instance
            Instance.Builder originInstanceBuilder = initializeOpenSourceSoftwareInstanceBuilder(url);
            originInstanceBuilder.setHostedby(collectedFromOrigin);
            originInstanceBuilder.setCollectedfrom(collectedFromSH);
            resultBuilder.addInstance(originInstanceBuilder.build());
            entityBuilder.addCollectedfrom(collectedFromOrigin);
            
            //SH instance
            Instance.Builder shInstanceBuilder = initializeOpenSourceSoftwareInstanceBuilder(meta.getSHUrl().toString());
            shInstanceBuilder.setHostedby(collectedFromSH);
            shInstanceBuilder.setCollectedfrom(collectedFromSH);
            resultBuilder.addInstance(shInstanceBuilder.build());
            entityBuilder.addCollectedfrom(collectedFromSH);

        } else {
            Instance.Builder instanceBuilder = initializeOpenSourceSoftwareInstanceBuilder(url);
            KeyValue hostedBy = buildHostedBy(meta.getRepositoryName().toString());
            instanceBuilder.setHostedby(hostedBy);
            instanceBuilder.setCollectedfrom(hostedBy);
            resultBuilder.addInstance(instanceBuilder.build());
            entityBuilder.addCollectedfrom(hostedBy);
        }
        
        entityBuilder.setResult(resultBuilder.build());
        return BuilderModuleHelper.buildOaf(entityBuilder.build(), meta.getConfidenceLevel(), INFERENCE_PROVENANCE);
    }
    
    private static Instance.Builder initializeOpenSourceSoftwareInstanceBuilder(String url) {
        Instance.Builder instanceBuilder = Instance.newBuilder();
        instanceBuilder.addUrl(url);
        instanceBuilder.setInstancetype(INSTANCE_TYPE_SOFTWARE);
        instanceBuilder.setAccessright(ACCESS_RIGHT_OPEN_SOURCE);
        return instanceBuilder;
    }

    private static StructuredProperty buildMainTitle(String softwareTitle, String repositoryName) {
        StructuredProperty.Builder titleBuilder = StructuredProperty.newBuilder();
        
        titleBuilder.setValue(MessageFormat.format(SOFTWARE_TITLE_MAIN_TEMPLATE, 
                new Object[] {softwareTitle, repositoryName}));
        
        Qualifier.Builder qualifierBuilder = Qualifier.newBuilder();
        qualifierBuilder.setClassid(InfoSpaceConstants.SEMANTIC_CLASS_MAIN_TITLE);
        qualifierBuilder.setClassname(InfoSpaceConstants.SEMANTIC_CLASS_MAIN_TITLE);
        qualifierBuilder.setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_TITLE);
        qualifierBuilder.setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_TITLE);
        titleBuilder.setQualifier(qualifierBuilder.build());
        
        return titleBuilder.build();
    }
    
    private static StructuredProperty buildContextualisedTitle(String softwareTitle, String repositoryName, String publicationTitle) {
        StructuredProperty.Builder titleBuilder = StructuredProperty.newBuilder();
        
        titleBuilder.setValue(MessageFormat.format(SOFTWARE_TITLE_ALTERNATIVE_TEMPLATE, 
                new Object[] {softwareTitle, repositoryName, publicationTitle}));
        
        Qualifier.Builder qualifierBuilder = Qualifier.newBuilder();
        qualifierBuilder.setClassid(InfoSpaceConstants.SEMANTIC_CLASS_ALTERNATIVE_TITLE);
        qualifierBuilder.setClassname(InfoSpaceConstants.SEMANTIC_CLASS_ALTERNATIVE_TITLE);
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
    
    private static Qualifier buildAccessRightOpenSource() {
        Qualifier.Builder qualifierBuilder = Qualifier.newBuilder();
        qualifierBuilder.setClassid(InfoSpaceConstants.SEMANTIC_CLASS_OPEN_SOURCE);
        qualifierBuilder.setClassname(InfoSpaceConstants.SEMANTIC_CLASS_NAME_OPEN_SOURCE);
        qualifierBuilder.setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_ACCESS_MODES);
        qualifierBuilder.setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_ACCESS_MODES);
        return qualifierBuilder.build();
    }
    
    private static KeyValue buildHostedBy(String repositoryName) {
        return buildHostedBy(generateDatasourceId(repositoryName), repositoryName);
    }
    
    private static KeyValue buildHostedBy(String key, String value) {
        KeyValue.Builder hostedByBuilder = KeyValue.newBuilder();
        hostedByBuilder.setKey(key);
        hostedByBuilder.setValue(value);
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
                isRepositoryAllowed(source.getRepositoryName()) &&
                (StringUtils.isNotBlank(source.getSoftwarePageURL()) || StringUtils.isNotBlank(source.getSoftwareUrl())));
    }
    
    private static boolean isRepositoryAllowed(CharSequence softwareRepository) {
        return StringUtils.isNotBlank(softwareRepository) && !SOFTWARE_REPOSITORY_OTHER.equals(softwareRepository);
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
        
        @Parameter(names = "-inputDocumentToSoftwareAvroPath", required = true)
        private String inputDocumentToSoftwareAvroPath;
        
        @Parameter(names = "-inputDocumentMetadataAvroPath", required = true)
        private String inputDocumentMetadataAvroPath;
        
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
