package eu.dnetlib.iis.wf.export.actionmanager.entity;

import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.common.utils.DateTimeUtils;
import eu.dnetlib.iis.common.utils.RDDUtils;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.DocumentToSoftwareUrlWithMeta;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import eu.dnetlib.iis.wf.export.actionmanager.AtomicActionSerializationUtils;
import eu.dnetlib.iis.wf.export.actionmanager.OafConstants;
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
    
    public static final String INFERENCE_PROVENANCE = InfoSpaceConstants.SEMANTIC_CLASS_IIS
            + InfoSpaceConstants.INFERENCE_PROVENANCE_SEPARATOR + AlgorithmName.document_software_url;
    
    private static final Qualifier RESULT_TYPE_SOFTWARE = buildResultTypeSoftware();
    
    private static final DataInfo OAF_ENTITY_DATAINFO = BuilderModuleHelper.buildInferenceForTrustLevel(false,
            StaticConfigurationProvider.ACTION_TRUST_0_9, INFERENCE_PROVENANCE, InfoSpaceConstants.SEMANTIC_CLASS_IIS);
    
    private static final Qualifier INSTANCE_TYPE_SOFTWARE = buildInstanceTypeSoftware();
    
    private static final AccessRight ACCESS_RIGHT_OPEN_SOURCE = buildAccessRightOpenSource();

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
        
        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
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
                    documentIdToTitle, job.getConfiguration(), params.outputEntityPath);

            JavaRDD<Tuple3<String, String, Float>> dedupedRelationTriples = handleRelations(documentToSoftwareUrlWithMetaFiltered, 
                    job.getConfiguration(), params.outputRelationPath);
            
            // reporting
            counterReporter.report(sc, dedupedSoftwareUrls, dedupedRelationTriples, params.outputReportPath);
        }
    }

    // ----------------------------------------- PRIVATE ----------------------------------------------
    
    private static JavaRDD<?> handleEntities(JavaRDD<DocumentToSoftwareUrlWithMeta> documentToSoftwareUrl, 
            JavaPairRDD<CharSequence, CharSequence> documentIdToTitle, Configuration jobConfig, String outputAvroPath) {
        
        // supplementing empty software titles with templates based on document->software relations
        JavaPairRDD<CharSequence, DocumentToSoftwareUrlWithMeta> documentToSoftwareByPublicationId = documentToSoftwareUrl.mapToPair(e -> new Tuple2<>(e.getDocumentId(), e));
        JavaPairRDD<CharSequence, Tuple2<DocumentToSoftwareUrlWithMeta, Optional<CharSequence>>> documentToSoftwareByPublicationIdJoined = documentToSoftwareByPublicationId.leftOuterJoin(documentIdToTitle);
        JavaRDD<Tuple2<DocumentToSoftwareUrlWithMeta, Set<CharSequence>>> documentToSoftwareUrlWithPubTitles = documentToSoftwareByPublicationIdJoined.map(e -> convertTitleToSetOfTitles(e._2._1, e._2._2));
        
        JavaPairRDD<String, Tuple2<DocumentToSoftwareUrlWithMeta, Set<CharSequence>>> documentToSoftwareWithPubTitlesByUrl = documentToSoftwareUrlWithPubTitles.mapToPair(e -> new Tuple2<>(pickUrl(e._1), e)); 

        JavaPairRDD<String, Tuple2<DocumentToSoftwareUrlWithMeta, Set<CharSequence>>> documentToSoftwareWithPubTitlesReduced = documentToSoftwareWithPubTitlesByUrl.reduceByKey((x, y) -> mergeSoftwareRecords(x,y));
        
        JavaRDD<Tuple2<DocumentToSoftwareUrlWithMeta, Set<CharSequence>>> documentToSoftwareReducedValues = documentToSoftwareWithPubTitlesReduced.values();
        // to be used by both entity exporter and reporter consumers
        documentToSoftwareReducedValues.cache();
        
        JavaRDD<AtomicAction<Software>> entityResult = documentToSoftwareReducedValues.map(SoftwareExporterJob::buildEntityAction);

        RDDUtils.saveTextPairRDD(AtomicActionSerializationUtils.mapActionToText(entityResult), numberOfOutputFiles,
                outputAvroPath, jobConfig);

        return documentToSoftwareReducedValues;
    }
    
    private static Tuple2<DocumentToSoftwareUrlWithMeta, Set<CharSequence>> convertTitleToSetOfTitles(DocumentToSoftwareUrlWithMeta meta, Optional<CharSequence> referencedPubTitle) {
        return new Tuple2<>(meta,
                referencedPubTitle.isPresent() && StringUtils.isNotBlank(referencedPubTitle.get())
                        ? Sets.<CharSequence>newHashSet(referencedPubTitle.get())
                        : Sets.<CharSequence>newHashSet());
    }
    
    private static JavaRDD<Tuple3<String, String, Float>> handleRelations(JavaRDD<DocumentToSoftwareUrlWithMeta> documentToSoftwareUrl, 
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

        JavaRDD<AtomicAction<Relation>> relationResult = dedupedRelationTriples
                .flatMap(x -> buildRelationActions(x._1(), x._2(), x._3()).iterator());
        
        RDDUtils.saveTextPairRDD(AtomicActionSerializationUtils.mapActionToText(relationResult), numberOfOutputFiles,
                outputAvroPath, jobConfig);

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
                InfoSpaceConstants.ID_NAMESPACE_SEPARATOR + DigestUtils.md5Hex(url);
    }

    private static String generateDatasourceId(String repositoryName) {
        return InfoSpaceConstants.ROW_PREFIX_DATASOURCE + InfoSpaceConstants.OPENAIRE_ENTITY_ID_PREFIX +
                InfoSpaceConstants.ID_NAMESPACE_SEPARATOR + DigestUtils.md5Hex(repositoryName);
    }

    private static AtomicAction<Software> buildEntityAction(Tuple2<DocumentToSoftwareUrlWithMeta, Set<CharSequence>> object) {
        AtomicAction<Software> action = new AtomicAction<>();
        action.setClazz(Software.class);
        action.setPayload(buildSoftwareOaf(object));
        return action;
    }
    
    private static Software buildSoftwareOaf(Tuple2<DocumentToSoftwareUrlWithMeta, Set<CharSequence>> metaWithPublicationTitles) {
        
        DocumentToSoftwareUrlWithMeta meta = metaWithPublicationTitles._1;
        
        String url = pickUrl(meta);
        
        Software software = new Software();
        software.setId(generateSoftwareEntityId(url));

        String dateStr = DateTimeUtils.format(LocalDateTime.now());
        software.setDateofcollection(dateStr);
        software.setDateoftransformation(dateStr);
        
        Field<String> urlField = new Field<>();
        urlField.setValue(url);
        software.setCodeRepositoryUrl(urlField);
        
        List<StructuredProperty> titles = Lists.newArrayList();

        titles.add(buildMainTitle(meta.getSoftwareTitle().toString(), meta.getRepositoryName().toString()));
        
        for (CharSequence currentPubTitle : metaWithPublicationTitles._2) {
            titles.add(buildContextualisedTitle(meta.getSoftwareTitle().toString(),
                    meta.getRepositoryName().toString(), currentPubTitle.toString()));
        }
        
        software.setTitle(titles);
        
        if (StringUtils.isNotBlank(meta.getSoftwareDescription())) {
            Field<String> descrField = new Field<>();
            descrField.setValue(meta.getSoftwareDescription().toString());
            software.setDescription(Collections.singletonList(descrField));
        }
        
        software.setResulttype(RESULT_TYPE_SOFTWARE);

        if (StringUtils.isNotBlank(meta.getSHUrl())) {
            //Software Heritage mode
            KeyValue collectedFromSH = buildHostedBy(generateDatasourceId(COLLECTED_FROM_SOFTWARE_HERITAGE_BASE_ID),
                    COLLECTED_FROM_SOFTWARE_HERITAGE_NAME);
            KeyValue collectedFromOrigin = buildHostedBy(meta.getRepositoryName().toString());

            List<Instance> instanceList = Lists.newArrayList();
            List<KeyValue> collectedFromList = Lists.newArrayList();
            
            //origin instance
            Instance originInstanceBuilder = initializeOpenSourceSoftwareInstanceBuilder(url);
            originInstanceBuilder.setHostedby(collectedFromOrigin);
            originInstanceBuilder.setCollectedfrom(collectedFromSH);
            instanceList.add(originInstanceBuilder);
            collectedFromList.add(collectedFromOrigin);
            
            //SH instance
            Instance shInstanceBuilder = initializeOpenSourceSoftwareInstanceBuilder(meta.getSHUrl().toString());
            shInstanceBuilder.setHostedby(collectedFromSH);
            shInstanceBuilder.setCollectedfrom(collectedFromSH);
            instanceList.add(shInstanceBuilder);
            collectedFromList.add(collectedFromSH);
            
            software.setInstance(instanceList);
            software.setCollectedfrom(collectedFromList);

        } else {
            Instance instanceBuilder = initializeOpenSourceSoftwareInstanceBuilder(url);
            KeyValue hostedBy = buildHostedBy(meta.getRepositoryName().toString());
            instanceBuilder.setHostedby(hostedBy);
            instanceBuilder.setCollectedfrom(hostedBy);
            software.setInstance(Collections.singletonList(instanceBuilder));
            software.setCollectedfrom(Collections.singletonList(hostedBy));
        }
        
        software.setDataInfo(OAF_ENTITY_DATAINFO);
        software.setLastupdatetimestamp(System.currentTimeMillis());

        return software;
    }
    
    private static Instance initializeOpenSourceSoftwareInstanceBuilder(String url) {
        Instance instance = new Instance();
        instance.setInstancetype(INSTANCE_TYPE_SOFTWARE);
        instance.setAccessright(ACCESS_RIGHT_OPEN_SOURCE);
        instance.setUrl(Collections.singletonList(url));
        return instance;
    }

    private static StructuredProperty buildMainTitle(String softwareTitle, String repositoryName) {
        return buildTitle(MessageFormat.format(SOFTWARE_TITLE_MAIN_TEMPLATE, softwareTitle, repositoryName),
                InfoSpaceConstants.SEMANTIC_CLASS_MAIN_TITLE);
    }
    
    private static StructuredProperty buildContextualisedTitle(String softwareTitle, String repositoryName,
            String publicationTitle) {
        return buildTitle(MessageFormat.format(SOFTWARE_TITLE_ALTERNATIVE_TEMPLATE, softwareTitle, repositoryName,
                publicationTitle), InfoSpaceConstants.SEMANTIC_CLASS_ALTERNATIVE_TITLE);
    }
    
    private static StructuredProperty buildTitle(String value, String titleType) {
        StructuredProperty titleStructuredProperty = new StructuredProperty();
        titleStructuredProperty.setValue(value);
        titleStructuredProperty.setQualifier(buildQualifier(titleType, titleType,
                InfoSpaceConstants.SEMANTIC_SCHEME_DNET_TITLE, InfoSpaceConstants.SEMANTIC_SCHEME_DNET_TITLE));
        return titleStructuredProperty;
    }
    
    private static Qualifier buildResultTypeSoftware() {
        return buildQualifier(InfoSpaceConstants.SEMANTIC_CLASS_SOFTWARE,
                InfoSpaceConstants.SEMANTIC_CLASS_SOFTWARE,
                InfoSpaceConstants.SEMANTIC_SCHEME_DNET_RESULT_TYPOLOGIES,
                InfoSpaceConstants.SEMANTIC_SCHEME_DNET_RESULT_TYPOLOGIES);
    }
    
    private static Qualifier buildInstanceTypeSoftware() {
        return buildQualifier(InfoSpaceConstants.SEMANTIC_CLASS_INSTANCE_TYPE_SOFTWARE,
                InfoSpaceConstants.SEMANTIC_CLASS_PUBLICATION_RESOURCE_SOFTWARE,
                InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PUBLICATION_RESOURCE,
                InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PUBLICATION_RESOURCE);
    }

    private static AccessRight buildAccessRightOpenSource() {
        AccessRight accessRight = new AccessRight();
        accessRight.setClassid(InfoSpaceConstants.SEMANTIC_CLASS_OPEN_SOURCE);
        accessRight.setClassname(InfoSpaceConstants.SEMANTIC_CLASS_NAME_OPEN_SOURCE);
        accessRight.setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_ACCESS_MODES);
        accessRight.setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_ACCESS_MODES);
        return accessRight;
    }
    
    private static Qualifier buildQualifier(String classid, String classname, String schemeid, String schemename) {
        Qualifier qualifier = new Qualifier();
        qualifier.setClassid(classid);
        qualifier.setClassname(classname);
        qualifier.setSchemeid(schemeid);
        qualifier.setSchemename(schemename);
        return qualifier;
    }
    
    private static KeyValue buildHostedBy(String repositoryName) {
        return buildHostedBy(generateDatasourceId(repositoryName), repositoryName);
    }
    
    private static KeyValue buildHostedBy(String key, String value) {
        KeyValue keyValue = new KeyValue();
        keyValue.setKey(key);
        keyValue.setValue(value);
        return keyValue;
    }
    
    private static List<AtomicAction<Relation>> buildRelationActions(String docId, String softId,
            Float confidenceLevel) {

        AtomicAction<Relation> forwardAction = new AtomicAction<>();
        forwardAction.setClazz(Relation.class);
        forwardAction.setPayload(buildRelation(docId, softId, confidenceLevel));

        AtomicAction<Relation> reverseAction = new AtomicAction<>();
        reverseAction.setClazz(Relation.class);
        reverseAction.setPayload(buildRelation(softId, docId, confidenceLevel));

        return Arrays.asList(forwardAction, reverseAction);
    }
    
    private static Relation buildRelation(String source, String target, Float confidenceLevel) {
        Relation relation = new Relation();
        relation.setRelType(OafConstants.REL_TYPE_RESULT_RESULT);
        relation.setSubRelType(OafConstants.SUBREL_TYPE_RELATIONSHIP);
        relation.setRelClass(OafConstants.REL_CLASS_ISRELATEDTO);
        
        relation.setSource(source);
        relation.setTarget(target);
        
        relation.setDataInfo(BuilderModuleHelper.buildInferenceForConfidenceLevel(confidenceLevel, INFERENCE_PROVENANCE));
        relation.setLastupdatetimestamp(System.currentTimeMillis());
        
        return relation;
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
        
        @Parameter(names = "-trustLevelThreshold", required = false)
        private String trustLevelThreshold;

    }
    
}
