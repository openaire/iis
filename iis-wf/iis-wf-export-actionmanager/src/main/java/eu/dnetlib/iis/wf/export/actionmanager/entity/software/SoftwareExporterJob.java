package eu.dnetlib.iis.wf.export.actionmanager.entity.software;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.common.utils.DateTimeUtils;
import eu.dnetlib.iis.common.utils.IteratorUtils;
import eu.dnetlib.iis.common.utils.RDDUtils;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.DocumentToSoftwareUrlWithMeta;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import eu.dnetlib.iis.wf.export.actionmanager.AtomicActionSerializationUtils;
import eu.dnetlib.iis.wf.export.actionmanager.IdentifierFactory;
import eu.dnetlib.iis.wf.export.actionmanager.OafConstants;
import eu.dnetlib.iis.wf.export.actionmanager.cfg.StaticConfigurationProvider;
import eu.dnetlib.iis.wf.export.actionmanager.entity.ConfidenceLevelUtils;
import eu.dnetlib.iis.wf.export.actionmanager.module.AlgorithmName;
import eu.dnetlib.iis.wf.export.actionmanager.module.BuilderModuleHelper;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import scala.Tuple2;
import scala.Tuple4;

import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.util.*;

/**
 * Software entity and relations exporter reading {@link DocumentToSoftwareUrlWithMeta} avro records and exporting them as entity and relation actions.
 *
 * @author mhorst
 */
public class SoftwareExporterJob {

    private SoftwareExporterJob() {
    }

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

    private static final int NUMBER_OF_OUTPUT_FILES = 10;

    private static SparkAvroLoader avroLoader = new SparkAvroLoader();

    private static SoftwareExportCounterReporter counterReporter = new SoftwareExportCounterReporter();

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

    public static void main(String[] args) throws Exception {
        JobParameters params = new JobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputEntityPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputRelationPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputReportPath);

            final Float confidenceLevelThreshold = ConfidenceLevelUtils
                    .evaluateConfidenceLevelThreshold(params.trustLevelThreshold);
            final String collectedFromValue = params.collectedFromValue;

            JavaRDD<DocumentToSoftwareUrlWithMeta> relMetaRDD = avroLoader
                    .loadJavaRDD(sc, params.inputDocumentToSoftwareUrlPath, DocumentToSoftwareUrlWithMeta.class);
            JavaRDD<ExtractedDocumentMetadataMergedWithOriginal> entMetaRDD = avroLoader
                    .loadJavaRDD(sc, params.inputDocumentMetadataPath, ExtractedDocumentMetadataMergedWithOriginal.class);

            JavaPairRDD<CharSequence, DocumentToSoftwareUrlWithMeta> validRelMetaByIdRDD = relMetaRDD
                    .filter(f -> isRelationValidAndAboveThreshold(f, confidenceLevelThreshold))
                    .mapToPair(x -> new Tuple2<>(x.getDocumentId(), x));

            JavaPairRDD<CharSequence, ExtractedDocumentMetadataMergedWithOriginal> validEntMetaByIdRDD = entMetaRDD
                    .filter(SoftwareExporterJob::isMetadataValid)
                    .mapToPair(e -> new Tuple2<>(e.getId(), e));

            JavaRDD<SoftwareExportMetadata> softwareExportMetaRDD = validRelMetaByIdRDD
                    .join(validEntMetaByIdRDD)
                    .values()
                    .map(x -> {
                        DocumentToSoftwareUrlWithMeta relMeta = x._1();
                        ExtractedDocumentMetadataMergedWithOriginal entMeta = x._2();
                        HashSet<CharSequence> title = StringUtils.isNotBlank(entMeta.getTitle()) ?
                                Sets.newHashSet(entMeta.getTitle()) : Sets.newHashSet();
                        String softwareUrl = pickUrl(relMeta);
                        return new SoftwareExportMetadata(relMeta, title, generateDocumentId(entMeta),
                                generateSoftwareId(softwareUrl), softwareUrl);
                    });

            JavaRDD<SoftwareExportMetadata> softwareExportMetaDedupRDD = deduplicateByHigherConfidenceLevel(
                    softwareExportMetaRDD).cache();

            Configuration configuration = sc.hadoopConfiguration();
            configuration.set(FileOutputFormat.COMPRESS, Boolean.TRUE.toString());
            configuration.set(FileOutputFormat.COMPRESS_TYPE, SequenceFile.CompressionType.BLOCK.name());

            JavaPairRDD<Text, Text> relationsToExportRDD = relationsToExport(softwareExportMetaDedupRDD, collectedFromValue);
            RDDUtils.saveTextPairRDD(relationsToExportRDD, NUMBER_OF_OUTPUT_FILES, params.outputRelationPath, configuration);

            JavaPairRDD<Text, Text> entitiesToExportRDD = entitiesToExport(softwareExportMetaDedupRDD);
            RDDUtils.saveTextPairRDD(entitiesToExportRDD, NUMBER_OF_OUTPUT_FILES, params.outputEntityPath, configuration);

            counterReporter.report(sc, softwareExportMetaDedupRDD, params.outputReportPath);
        }
    }

    // ----------------------------------------- PRIVATE ----------------------------------------------

    private static boolean isRelationValidAndAboveThreshold(DocumentToSoftwareUrlWithMeta relation, Float confidenceLevelThreshold) {
        return isRelationValid(relation) &&
                ConfidenceLevelUtils.isValidConfidenceLevel(relation.getConfidenceLevel(), confidenceLevelThreshold);
    }

    private static boolean isRelationValid(DocumentToSoftwareUrlWithMeta source) {
        return StringUtils.isNotBlank(source.getSoftwareTitle()) && isRepositoryAllowed(source.getRepositoryName()) &&
                (StringUtils.isNotBlank(source.getSoftwarePageURL()) || StringUtils.isNotBlank(source.getSoftwareUrl()));
    }

    private static boolean isRepositoryAllowed(CharSequence softwareRepository) {
        return StringUtils.isNotBlank(softwareRepository) && !SOFTWARE_REPOSITORY_OTHER.contentEquals(softwareRepository);
    }

    private static boolean isMetadataValid(ExtractedDocumentMetadataMergedWithOriginal meta) {
        return StringUtils.isNotBlank(meta.getTitle());
    }

    private static String pickUrl(DocumentToSoftwareUrlWithMeta source) {
        return StringUtils.isNotBlank(source.getSoftwarePageURL()) ? source.getSoftwarePageURL().toString()
                : source.getSoftwareUrl().toString();
    }

    private static String generateDocumentId(ExtractedDocumentMetadataMergedWithOriginal meta) {
        return meta.getId().toString();
    }

    private static String generateSoftwareId(String softwareUrl) {
        return InfoSpaceConstants.ROW_PREFIX_RESULT + InfoSpaceConstants.OPENAIRE_ENTITY_ID_PREFIX +
                InfoSpaceConstants.ID_NAMESPACE_SEPARATOR + DigestUtils.md5Hex(softwareUrl);
    }

    private static JavaRDD<SoftwareExportMetadata> deduplicateByHigherConfidenceLevel(JavaRDD<SoftwareExportMetadata> rdd) {
        return rdd
                .groupBy(x -> new Tuple2<>(x.getDocumentId(), x.getSoftwareId()))
                .mapValues(xs -> IteratorUtils.toStream(xs.iterator()).reduce(SoftwareExporterJob::reduceByHigherConfidenceLevel))
                .filter(x -> x._2.isPresent())
                .mapValues(java.util.Optional::get)
                .values();
    }

    private static SoftwareExportMetadata reduceByHigherConfidenceLevel(SoftwareExportMetadata x, SoftwareExportMetadata y) {
        return x.getDocumentToSoftwareUrlWithMeta().getConfidenceLevel() >
                y.getDocumentToSoftwareUrlWithMeta().getConfidenceLevel() ? x : y;
    }

    private static JavaPairRDD<Text, Text> relationsToExport(JavaRDD<SoftwareExportMetadata> rdd, String collectedFromValue) {
        return AtomicActionSerializationUtils
                .mapActionToText(rdd
                        .flatMap(x -> {
                            String documentId = x.getDocumentId();
                            String softwareId = x.getSoftwareId();
                            Float confidenceLevel = x.getDocumentToSoftwareUrlWithMeta().getConfidenceLevel();
                            return buildRelationActions(documentId, softwareId, confidenceLevel, collectedFromValue).iterator();
                        })
                );
    }

    private static List<AtomicAction<Relation>> buildRelationActions(String documentId, String softwareId,
            Float confidenceLevel, String collectedFromValue) {
        AtomicAction<Relation> forwardAction = new AtomicAction<>();
        forwardAction.setClazz(Relation.class);
        forwardAction.setPayload(buildRelation(documentId, softwareId, confidenceLevel, collectedFromValue));

        AtomicAction<Relation> reverseAction = new AtomicAction<>();
        reverseAction.setClazz(Relation.class);
        reverseAction.setPayload(buildRelation(softwareId, documentId, confidenceLevel, collectedFromValue));

        return Arrays.asList(forwardAction, reverseAction);
    }

    private static Relation buildRelation(String source, String target, Float confidenceLevel, String collectedFromValue) {
        return BuilderModuleHelper.createRelation(source, target, OafConstants.REL_TYPE_RESULT_RESULT,
                OafConstants.SUBREL_TYPE_RELATIONSHIP, OafConstants.REL_CLASS_ISRELATEDTO, 
                BuilderModuleHelper.buildInferenceForConfidenceLevel(confidenceLevel, INFERENCE_PROVENANCE),
                collectedFromValue);
    }

    private static JavaPairRDD<Text, Text> entitiesToExport(JavaRDD<SoftwareExportMetadata> rdd) {
        JavaRDD<AtomicAction<Software>> atomicActionRDD = rdd
                .groupBy(x -> new Tuple4<>(x.getDocumentToSoftwareUrlWithMeta(), x.getDocumentId(), x.getSoftwareId(), x.getSoftwareUrl()))
                .mapValues(xs -> IteratorUtils.toStream(xs.iterator()).reduce(SoftwareExporterJob::mergeSoftwareExportMetadataRecords))
                .values()
                .filter(java.util.Optional::isPresent)
                .map(java.util.Optional::get)
                .map(x -> {
                    DocumentToSoftwareUrlWithMeta relation = x.getDocumentToSoftwareUrlWithMeta();
                    Set<CharSequence> titles = x.getTitle();
                    String softwareId = x.getSoftwareId();
                    String softwareUrl = x.getSoftwareUrl();
                    return buildEntityAction(relation, titles, softwareId, softwareUrl);
                });

        return AtomicActionSerializationUtils.mapActionToText(atomicActionRDD);
    }

    private static SoftwareExportMetadata mergeSoftwareExportMetadataRecords(SoftwareExportMetadata left,
                                                                             SoftwareExportMetadata right) {
        // assuming DocumentToSoftwareUrlWithMeta holds the same set of software related fields in both tuples (since the url was the same)
        return new SoftwareExportMetadata(left.getDocumentToSoftwareUrlWithMeta(), mergeTitles(left.getTitle(), right.getTitle()),
                left.getDocumentId(), left.getSoftwareId(), left.getSoftwareUrl());
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

    private static AtomicAction<Software> buildEntityAction(DocumentToSoftwareUrlWithMeta relation,
                                                            Set<CharSequence> titles,
                                                            String softwareId,
                                                            String softwareUrl) {
        AtomicAction<Software> action = new AtomicAction<>();
        action.setClazz(Software.class);
        action.setPayload(buildOafInstance(relation, titles, softwareId, softwareUrl));
        return action;
    }

    private static Software buildOafInstance(DocumentToSoftwareUrlWithMeta relation,
                                             Set<CharSequence> titles,
                                             String softwareId,
                                             String softwareUrl) {
        Software software = new Software();

        String dateStr = DateTimeUtils.format(LocalDateTime.now());
        software.setDateofcollection(dateStr);
        software.setDateoftransformation(dateStr);

        Field<String> urlField = new Field<>();
        urlField.setValue(softwareUrl);
        software.setCodeRepositoryUrl(urlField);

        List<StructuredProperty> title = Lists.newArrayList();

        title.add(buildMainTitle(relation.getSoftwareTitle().toString(), relation.getRepositoryName().toString()));

        for (CharSequence currentPubTitle : titles) {
            title.add(buildContextualisedTitle(relation.getSoftwareTitle().toString(),
                    relation.getRepositoryName().toString(), currentPubTitle.toString()));
        }

        software.setTitle(title);

        if (StringUtils.isNotBlank(relation.getSoftwareDescription())) {
            Field<String> descrField = new Field<>();
            descrField.setValue(relation.getSoftwareDescription().toString());
            software.setDescription(Collections.singletonList(descrField));
        }

        software.setResulttype(RESULT_TYPE_SOFTWARE);

        if (StringUtils.isNotBlank(relation.getSHUrl())) {
            //Software Heritage mode
            KeyValue collectedFromSH = buildHostedBy(generateDatasourceId(COLLECTED_FROM_SOFTWARE_HERITAGE_BASE_ID),
                    COLLECTED_FROM_SOFTWARE_HERITAGE_NAME);
            KeyValue collectedFromOrigin = buildHostedBy(relation.getRepositoryName().toString());

            List<Instance> instanceList = Lists.newArrayList();
            List<KeyValue> collectedFromList = Lists.newArrayList();

            //origin instance
            Instance originInstanceBuilder = initializeOpenSourceSoftwareInstanceBuilder(softwareUrl);
            originInstanceBuilder.setHostedby(collectedFromOrigin);
            originInstanceBuilder.setCollectedfrom(collectedFromSH);
            instanceList.add(originInstanceBuilder);
            collectedFromList.add(collectedFromOrigin);

            //SH instance
            Instance shInstanceBuilder = initializeOpenSourceSoftwareInstanceBuilder(relation.getSHUrl().toString());
            shInstanceBuilder.setHostedby(collectedFromSH);
            shInstanceBuilder.setCollectedfrom(collectedFromSH);
            instanceList.add(shInstanceBuilder);
            collectedFromList.add(collectedFromSH);

            software.setInstance(instanceList);
            software.setCollectedfrom(collectedFromList);

        } else {
            Instance instanceBuilder = initializeOpenSourceSoftwareInstanceBuilder(softwareUrl);
            KeyValue hostedBy = buildHostedBy(relation.getRepositoryName().toString());
            instanceBuilder.setHostedby(hostedBy);
            instanceBuilder.setCollectedfrom(hostedBy);
            software.setInstance(Collections.singletonList(instanceBuilder));
            software.setCollectedfrom(Collections.singletonList(hostedBy));
        }

        software.setDataInfo(OAF_ENTITY_DATAINFO);
        software.setLastupdatetimestamp(System.currentTimeMillis());

        setId(software, softwareId);

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

    private static String generateDatasourceId(String repositoryName) {
        return InfoSpaceConstants.ROW_PREFIX_DATASOURCE + InfoSpaceConstants.OPENAIRE_ENTITY_ID_PREFIX +
                InfoSpaceConstants.ID_NAMESPACE_SEPARATOR + DigestUtils.md5Hex(repositoryName);
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

    private static void setId(Software software, String fallbackId) {
        software.setId(fallbackId);
        software.setId(IdentifierFactory.createIdentifier(software));
    }

    @Parameters(separators = "=")
    private static class JobParameters {

        @Parameter(names = "-inputDocumentToSoftwareUrlPath", required = true)
        private String inputDocumentToSoftwareUrlPath;

        @Parameter(names = "-inputDocumentMetadataPath", required = true)
        private String inputDocumentMetadataPath;

        @Parameter(names = "-outputEntityPath", required = true)
        private String outputEntityPath;

        @Parameter(names = "-outputRelationPath", required = true)
        private String outputRelationPath;

        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;

        @Parameter(names = "-trustLevelThreshold", required = false)
        private String trustLevelThreshold;
        
        @Parameter(names = "-collectedFromValue", required = true)
        private String collectedFromValue;
    }
}
