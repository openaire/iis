package eu.dnetlib.iis.wf.export.actionmanager.entity.patent;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import eu.dnetlib.actionmanager.actions.ActionFactory;
import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.data.mapreduce.util.OafDecoder;
import eu.dnetlib.data.proto.*;
import eu.dnetlib.data.transform.xml.AbstractDNetXsltFunctions;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.java.stream.ListUtils;
import eu.dnetlib.iis.common.java.stream.StreamUtils;
import eu.dnetlib.iis.common.utils.DateTimeUtils;
import eu.dnetlib.iis.common.utils.RDDUtils;
import eu.dnetlib.iis.referenceextraction.patent.schemas.DocumentToPatent;
import eu.dnetlib.iis.referenceextraction.patent.schemas.HolderCountry;
import eu.dnetlib.iis.referenceextraction.patent.schemas.Patent;
import eu.dnetlib.iis.wf.export.actionmanager.cfg.StaticConfigurationProvider;
import eu.dnetlib.iis.wf.export.actionmanager.entity.ConfidenceLevelUtils;
import eu.dnetlib.iis.wf.export.actionmanager.module.AlgorithmName;
import eu.dnetlib.iis.wf.export.actionmanager.module.BuilderModuleHelper;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import scala.Tuple2;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Patent entity and relations exporter reading {@link DocumentToPatent} avro records and exporting them as entity and relation actions.
 *
 * @author pjacewicz
 */
public class PatentExporterJob {
    private static final String EPO = "EPO";
    private static final String EUROPEAN_PATENT_OFFICE__PATSTAT = "European Patent Office/PATSTAT";
    private static final String CLASS_EPO_ID = "epo_id";
    private static final String CLASS_EPO_NR_EPODOC = "epo_nr_epodoc";
    private static final String IPC = "IPC";
    private static final String INTERNATIONAL_PATENT_CLASSIFICATION = "International Patent Classification";
    private static final String PATENT_ENTITY_ID_PREFIX = "epopatstat__";
    private static final String INFERENCE_PROVENANCE = buildInferenceProvenance();
    private static final String PATENT_DATASOURCE_OPENAIRE_ID_PREFIX = InfoSpaceConstants.ROW_PREFIX_DATASOURCE + InfoSpaceConstants.OPENAIRE_ENTITY_ID_PREFIX + InfoSpaceConstants.ID_NAMESPACE_SEPARATOR;
    private static final String PATENT_RESULT_OPENAIRE_ID_PREFIX = InfoSpaceConstants.ROW_PREFIX_RESULT + PATENT_ENTITY_ID_PREFIX + InfoSpaceConstants.ID_NAMESPACE_SEPARATOR;
    private static final String PATENT_ID_PREFIX_EPO = buildRowPrefixDatasourceOpenaireEntityIdPrefixEpo();
    private static final ResultResultProtos.ResultResult OAFREL_RESULTRESULT = buildOafRelResultResult();
    private static final FieldTypeProtos.KeyValue OAF_ENTITY_COLLECTEDFROM = buildOafEntityPatentKeyValue();
    private static final FieldTypeProtos.Qualifier OAF_ENTITY_RESULT_METADATA_RESULTTYPE = buildOafEntityResultMetadataResulttype();
    private static final FieldTypeProtos.Qualifier OAF_ENTITY_PID_QUALIFIER_CLASS_EPO_ID = buildOafEntityPidQualifierClassEpoId();
    private static final FieldTypeProtos.Qualifier OAF_ENTITY_PID_QUALIFIER_CLASS_EPO_NR_EPODOC = buildOafEntityPidQualifierClassEpoNrEpodoc();
    private static final FieldTypeProtos.Qualifier OAF_ENTITY_RESULT_METADATA_TITLE_QUALIFIER = buildOafEntityResultMetadataTitleQualifier();
    private static final FieldTypeProtos.Qualifier OAF_ENTITY_RESULT_METADATA_SUBJECT_QUALIFIER = buildOafEntityResultMetadataSubjectQualifier();
    private static final FieldTypeProtos.Qualifier OAF_ENTITY_RESULT_METADATA_RELEVANTDATE_QUALIFIER = buildOafEntityResultMetadataRelevantdateQualifier();

    private static final SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static final int numberOfOutputFiles = 10;
    private static final ActionFactory actionFactory = new ActionFactory();
    private static final PatentExportCounterReporter counterReporter = new PatentExportCounterReporter();

    private static final DateTimeFormatter PATENT_DATE_OF_COLLECTION_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws IOException {
        JobParameters params = new JobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        Configuration configuration = Job.getInstance().getConfiguration();
        configuration.set(FileOutputFormat.COMPRESS, Boolean.TRUE.toString());
        configuration.set(FileOutputFormat.COMPRESS_TYPE, SequenceFile.CompressionType.BLOCK.name());

        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");

        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputRelationPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputEntityPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputReportPath);

            Float trustLevelThreshold = ConfidenceLevelUtils.evaluateConfidenceLevelThreshold(params.trustLevelThreshold);

            JavaRDD<DocumentToPatent> documentToPatents = avroLoader
                    .loadJavaRDD(sc, params.inputDocumentToPatentPath, DocumentToPatent.class);
            JavaRDD<Patent> patents = avroLoader
                    .loadJavaRDD(sc, params.inputPatentPath, Patent.class);

            JavaRDD<DocumentToPatent> documentToPatentsToExport =
                    documentToPatentsToExport(documentToPatents, trustLevelThreshold);

            JavaPairRDD<CharSequence, Patent> patentsById = patents
                    .mapToPair(x -> new Tuple2<>(x.getApplnId(), x))
                    .cache();

            JavaRDD<DocumentToPatentWithIdsToExport> documentToPatentsToExportWithIds = documentToPatentsToExport
                    .mapToPair(x -> new Tuple2<>(x.getPatentId(), x))
                    .join(patentsById)
                    .map(x -> {
                        DocumentToPatent documentToPatent = x._2()._1();
                        Patent patent = x._2()._2();
                        return new DocumentToPatentWithIdsToExport(x._2()._1(), documentIdToExport(documentToPatent.getDocumentId()),
                                patentIdToExport(patent.getApplnAuth(), patent.getApplnNr()));
                    })
                    .cache();

            JavaPairRDD<Text, Text> relationsToExport =
                    relationsToExport(documentToPatentsToExportWithIds, params.relationActionSetId);
            RDDUtils.saveTextPairRDD(relationsToExport, numberOfOutputFiles, params.outputRelationPath, configuration);

            String patentDateOfCollection = DateTimeUtils.format(
                    LocalDateTime.parse(params.patentDateOfCollection, PATENT_DATE_OF_COLLECTION_FORMATTER));
            JavaPairRDD<Text, Text> entitiesToExport =
                    entitiesToExport(documentToPatentsToExportWithIds, patentsById, patentDateOfCollection,
                            params.patentEpoUrlRoot, params.entityActionSetId);
            RDDUtils.saveTextPairRDD(entitiesToExport, numberOfOutputFiles, params.outputEntityPath, configuration);

            counterReporter.report(sc, documentToPatentsToExportWithIds, params.outputReportPath);
        }
    }

    //------------------------ PRIVATE --------------------------

    private static String buildInferenceProvenance() {
        return InfoSpaceConstants.SEMANTIC_CLASS_IIS + InfoSpaceConstants.INFERENCE_PROVENANCE_SEPARATOR + AlgorithmName.document_patent;
    }

    private static String buildRowPrefixDatasourceOpenaireEntityIdPrefixEpo() {
        return appendMd5(PATENT_DATASOURCE_OPENAIRE_ID_PREFIX, EPO);
    }

    private static ResultResultProtos.ResultResult buildOafRelResultResult() {
        return ResultResultProtos.ResultResult.newBuilder()
                .setRelationship(buildOafRelResultResultRelationship())
                .build();
    }

    private static ResultResultProtos.ResultResult.Relationship buildOafRelResultResultRelationship() {
        return ResultResultProtos.ResultResult.Relationship.newBuilder()
                .setRelMetadata(BuilderModuleHelper.buildRelMetadata(
                        InfoSpaceConstants.SEMANTIC_SCHEME_DNET_RELATIONS_RESULT_RESULT, ResultResultProtos.ResultResult.Relationship.RelName.isRelatedTo.name()))
                .build();
    }

    private static FieldTypeProtos.Qualifier buildOafEntityResultMetadataResulttype() {
        return FieldTypeProtos.Qualifier.newBuilder()
                .setClassid(InfoSpaceConstants.SEMANTIC_CLASS_PUBLICATION)
                .setClassname(InfoSpaceConstants.SEMANTIC_CLASS_PUBLICATION)
                .setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_RESULT_TYPOLOGIES)
                .setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_RESULT_TYPOLOGIES)
                .build();
    }

    private static ResultProtos.Result.Instance buildOafEntityResultInstance(Patent patent, String patentEpoUrlRoot) {
        return ResultProtos.Result.Instance.newBuilder()
                .setInstancetype(buildOafEntityResultInstanceInstancetype())
                .setHostedby(buildOafEntityPatentKeyValue())
                .setCollectedfrom(buildOafEntityPatentKeyValue())
                .addUrl(buildOafEntityResultInstanceUrl(patent, patentEpoUrlRoot))
                .build();
    }

    private static String buildOafEntityResultInstanceUrl(Patent patent, String patentEpoUrlRoot) {
        return String.format("%s%s%s", patentEpoUrlRoot, patent.getApplnAuth(), patent.getApplnNr());
    }

    private static FieldTypeProtos.Qualifier buildOafEntityResultInstanceInstancetype() {
        return FieldTypeProtos.Qualifier.newBuilder()
                .setClassid(InfoSpaceConstants.SEMANTIC_CLASS_INSTANCE_TYPE_PATENT)
                .setClassname(InfoSpaceConstants.SEMANTIC_CLASS_PATENT)
                .setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PUBLICATION_RESOURCE)
                .setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PUBLICATION_RESOURCE)
                .build();
    }

    private static FieldTypeProtos.KeyValue buildOafEntityPatentKeyValue() {
        return FieldTypeProtos.KeyValue.newBuilder()
                .setKey(PATENT_ID_PREFIX_EPO)
                .setValue(EUROPEAN_PATENT_OFFICE__PATSTAT)
                .build();
    }

    private static FieldTypeProtos.Qualifier buildOafEntityPidQualifierClassEpoId() {
        return FieldTypeProtos.Qualifier.newBuilder()
                .setClassid(CLASS_EPO_ID)
                .setClassname(CLASS_EPO_ID)
                .setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PID_TYPES)
                .setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PID_TYPES)
                .build();
    }

    private static FieldTypeProtos.Qualifier buildOafEntityPidQualifierClassEpoNrEpodoc() {
        return FieldTypeProtos.Qualifier.newBuilder()
                .setClassid(CLASS_EPO_NR_EPODOC)
                .setClassname(CLASS_EPO_NR_EPODOC)
                .setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PID_TYPES)
                .setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PID_TYPES)
                .build();
    }

    private static FieldTypeProtos.Qualifier buildOafEntityResultMetadataTitleQualifier() {
        return FieldTypeProtos.Qualifier.newBuilder()
                .setClassid(InfoSpaceConstants.SEMANTIC_CLASS_MAIN_TITLE)
                .setClassname(InfoSpaceConstants.SEMANTIC_CLASS_MAIN_TITLE)
                .setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_TITLE)
                .setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_TITLE)
                .build();
    }

    private static FieldTypeProtos.Qualifier buildOafEntityResultMetadataSubjectQualifier() {
        return FieldTypeProtos.Qualifier.newBuilder()
                .setClassid(IPC)
                .setClassname(INTERNATIONAL_PATENT_CLASSIFICATION)
                .setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_CLASSIFICATION_TAXONOMIES)
                .setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_CLASSIFICATION_TAXONOMIES)
                .build();
    }

    private static FieldTypeProtos.Qualifier buildOafEntityResultMetadataRelevantdateQualifier() {
        return FieldTypeProtos.Qualifier.newBuilder()
                .setClassid(InfoSpaceConstants.SEMANTIC_CLASS_NAME_SUBMITTED)
                .setClassname(InfoSpaceConstants.SEMANTIC_CLASS_NAME_SUBMITTED)
                .setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_DATACITE_DATE)
                .setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_DATACITE_DATE)
                .build();
    }

    private static JavaRDD<DocumentToPatent> documentToPatentsToExport(JavaRDD<DocumentToPatent> documentToPatents,
                                                                       Float trustLevelThreshold) {
        return documentToPatents
                .filter(x -> isValidDocumentToPatent(x, trustLevelThreshold))
                .groupBy(x -> new Tuple2<>(x.getDocumentId(), x.getPatentId()))
                .mapValues(xs -> StreamUtils.asStream(xs.iterator()).reduce(PatentExporterJob::reduceByConfidenceLevel))
                .filter(x -> x._2.isPresent())
                .mapValues(java.util.Optional::get)
                .values();
    }

    private static Boolean isValidDocumentToPatent(DocumentToPatent documentToPatent, Float trustLevelThreshold) {
        return ConfidenceLevelUtils.isValidConfidenceLevel(documentToPatent.getConfidenceLevel(), trustLevelThreshold);
    }

    private static DocumentToPatent reduceByConfidenceLevel(DocumentToPatent x, DocumentToPatent y) {
        if (x.getConfidenceLevel() > y.getConfidenceLevel()) {
            return x;
        }
        return y;
    }

    private static JavaPairRDD<Text, Text> relationsToExport(JavaRDD<DocumentToPatentWithIdsToExport> documentToPatentsToExportWithIds,
                                                             String relationActionSetId) {
        return documentToPatentsToExportWithIds
                .flatMap(x -> {
                    DocumentToPatent documentToPatent = x.getDocumentToPatent();
                    String documentIdToExport = x.getDocumentIdToExport();
                    String patentIdToExport = x.getPatentIdToExport();
                    return buildRelationActions(documentToPatent, documentIdToExport, patentIdToExport, relationActionSetId);
                })
                .mapToPair(PatentExporterJob::actionToTuple);
    }

    private static List<AtomicAction> buildRelationActions(DocumentToPatent documentToPatent,
                                                           String documentIdToExport,
                                                           String patentIdToExport,
                                                           String relationActionSetId) {
        OafProtos.Oaf.Builder builder = builderForRelationOaf(documentToPatent, documentIdToExport, patentIdToExport);

        OafProtos.Oaf forwardOaf = builder.build();
        AtomicAction forwardAction = actionFactory.createAtomicAction(
                relationActionSetId,
                StaticConfigurationProvider.AGENT_DEFAULT,
                documentIdToExport,
                OafDecoder.decode(forwardOaf).getCFQ(),
                patentIdToExport,
                forwardOaf.toByteArray());

        OafProtos.Oaf reverseOaf = BuilderModuleHelper.invertBidirectionalRelationAndBuild(builder);
        AtomicAction reverseAction = actionFactory.createAtomicAction(
                relationActionSetId,
                StaticConfigurationProvider.AGENT_DEFAULT,
                patentIdToExport,
                OafDecoder.decode(reverseOaf).getCFQ(),
                documentIdToExport,
                reverseOaf.toByteArray());

        return Arrays.asList(forwardAction, reverseAction);
    }

    private static OafProtos.Oaf.Builder builderForRelationOaf(DocumentToPatent documentToPatent,
                                                               String documentIdToExport,
                                                               String patentIdToExport) {
        return OafProtos.Oaf.newBuilder()
                .setKind(KindProtos.Kind.relation)
                .setRel(buildOafRel(documentIdToExport, patentIdToExport))
                .setDataInfo(BuilderModuleHelper.buildInferenceForConfidenceLevel(documentToPatent.getConfidenceLevel(), INFERENCE_PROVENANCE))
                .setLastupdatetimestamp(System.currentTimeMillis());
    }

    private static OafProtos.OafRel buildOafRel(String documentIdToExport, String patentIdToExport) {
        return OafProtos.OafRel.newBuilder()
                .setRelType(RelTypeProtos.RelType.resultResult)
                .setSubRelType(RelTypeProtos.SubRelType.relationship)
                .setRelClass(ResultResultProtos.ResultResult.Relationship.RelName.isRelatedTo.name())
                .setSource(documentIdToExport)
                .setTarget(patentIdToExport)
                .setChild(false)
                .setResultResult(OAFREL_RESULTRESULT)
                .build();
    }

    private static JavaPairRDD<Text, Text> entitiesToExport(JavaRDD<DocumentToPatentWithIdsToExport> documentToPatentsToExportWithIds,
                                                            JavaPairRDD<CharSequence, Patent> patentsById,
                                                            String patentDateOfCollection,
                                                            String patentEpoUrlRoot,
                                                            String entityActionSetId) {
        return documentToPatentsToExportWithIds
                .mapToPair(x -> new Tuple2<>(x.getDocumentToPatent().getPatentId(), x.getPatentIdToExport()))
                .distinct()
                .join(patentsById)
                .values()
                .map(x -> {
                    String patentIdToExport = x._1();
                    Patent patent = x._2();
                    return buildEntityAction(patent, patentIdToExport, patentDateOfCollection, patentEpoUrlRoot,
                            entityActionSetId);
                })
                .mapToPair(PatentExporterJob::actionToTuple);
    }

    private static AtomicAction buildEntityAction(Patent patent, String patentIdToExport, String patentDateOfCollection,
                                                  String patentEpoUrlRoot, String entityActionSetId) {
        OafProtos.Oaf oaf = buildEntityOaf(patent, patentIdToExport, patentDateOfCollection, patentEpoUrlRoot);
        return actionFactory.createAtomicAction(
                entityActionSetId,
                StaticConfigurationProvider.AGENT_DEFAULT,
                patentIdToExport,
                OafDecoder.decode(oaf).getCFQ(),
                InfoSpaceConstants.QUALIFIER_BODY_STRING,
                oaf.toByteArray()
        );
    }

    private static OafProtos.Oaf buildEntityOaf(Patent patent, String patentIdToExport, String patentDateOfCollection,
                                                String patentEpoUrlRoot) {
        return OafProtos.Oaf.newBuilder()
                .setKind(KindProtos.Kind.entity)
                .setEntity(buildOafEntity(patent, patentIdToExport, patentDateOfCollection, patentEpoUrlRoot))
                .setLastupdatetimestamp(System.currentTimeMillis())
                .build();
    }

    private static OafProtos.OafEntity buildOafEntity(Patent patent, String patentIdToExport, String patentDateOfCollection,
                                                      String patentEpoUrlRoot) {
        return OafProtos.OafEntity.newBuilder()
                .setType(TypeProtos.Type.result)
                .setId(patentIdToExport)
                .addCollectedfrom(OAF_ENTITY_COLLECTEDFROM)
                .addPid(buildOafEntityPid(String.format("%s%s", patent.getApplnAuth(), patent.getApplnNr()), OAF_ENTITY_PID_QUALIFIER_CLASS_EPO_ID))
                .addPid(buildOafEntityPid(patent.getApplnNrEpodoc().toString(), OAF_ENTITY_PID_QUALIFIER_CLASS_EPO_NR_EPODOC))
                .setDateofcollection(patentDateOfCollection)
                .setDateoftransformation(patentDateOfCollection)
                .setResult(buildOafEntityResult(patent, patentEpoUrlRoot))
                .build();
    }

    private static FieldTypeProtos.StructuredProperty buildOafEntityPid(String value, FieldTypeProtos.Qualifier qualifier) {
        return FieldTypeProtos.StructuredProperty.newBuilder()
                .setValue(value)
                .setQualifier(qualifier)
                .build();
    }

    private static ResultProtos.Result buildOafEntityResult(Patent patent, String patentEpoUrlRoot) {
        return ResultProtos.Result.newBuilder()
                .setMetadata(buildOafEntityResultMetadata(patent))
                .addInstance(buildOafEntityResultInstance(patent, patentEpoUrlRoot))
                .build();
    }

    private static ResultProtos.Result.Metadata buildOafEntityResultMetadata(Patent patent) {
        ResultProtos.Result.Metadata.Builder builder = ResultProtos.Result.Metadata.newBuilder();

        if (StringUtils.isNotBlank(patent.getApplnTitle())) {
            builder.addTitle(buildOafEntityResultMetadataTitle(patent.getApplnTitle()));
        }

        if (StringUtils.isNotBlank(patent.getApplnAbstract())) {
            builder.addDescription(buildOafEntityResultMetadataDescription(patent.getApplnAbstract()));
        }

        if (StringUtils.isNotBlank(patent.getEarliestPublnDate())) {
            builder.setDateofacceptance(buildOafEntityResultMetadataDateofacceptance(patent.getEarliestPublnDate()));
        }

        if (StringUtils.isNotBlank(patent.getApplnFilingDate())) {
            builder.addRelevantdate(buildOafEntityResultMetadataRelevantdate(patent.getApplnFilingDate()));
        }

        if (Objects.nonNull(patent.getIpcClassSymbol())) {
            builder.addAllSubject(buildOafEntityResultMetadataSubjects(patent.getIpcClassSymbol()));
        }

        if (Objects.nonNull(patent.getHolderCountry())) {
            builder.addAllAuthor(buildOafEntityResultMetadataAuthors(patent.getHolderCountry()));
            builder.addAllCountry(buildOafEntityResultMetadataCountries(patent.getHolderCountry()));
        }

        return builder
                .setResulttype(OAF_ENTITY_RESULT_METADATA_RESULTTYPE)
                .build();
    }

    private static FieldTypeProtos.StructuredProperty buildOafEntityResultMetadataTitle(CharSequence applnTitle) {
        return FieldTypeProtos.StructuredProperty.newBuilder()
                .setValue(applnTitle.toString())
                .setQualifier(OAF_ENTITY_RESULT_METADATA_TITLE_QUALIFIER)
                .build();
    }

    private static FieldTypeProtos.StringField buildOafEntityResultMetadataDescription(CharSequence applnAbstract) {
        return FieldTypeProtos.StringField.newBuilder()
                .setValue(applnAbstract.toString())
                .build();
    }

    private static String documentIdToExport(CharSequence documentId) {
        return documentId.toString();
    }

    private static String patentIdToExport(CharSequence applnAuth, CharSequence applnNr) {
        return appendMd5(PATENT_RESULT_OPENAIRE_ID_PREFIX, applnAuth.toString() + applnNr.toString());
    }

    private static List<FieldTypeProtos.StructuredProperty> buildOafEntityResultMetadataSubjects(List<CharSequence> ipcClassSymbols) {
        return ipcClassSymbols.stream()
                .filter(StringUtils::isNotBlank)
                .map(PatentExporterJob::buildOafEntityResultMetadataSubject)
                .collect(Collectors.toList());
    }

    private static FieldTypeProtos.StructuredProperty buildOafEntityResultMetadataSubject(CharSequence ipcClassSymbol) {
        return FieldTypeProtos.StructuredProperty.newBuilder()
                .setValue(ipcClassSymbol.toString())
                .setQualifier(OAF_ENTITY_RESULT_METADATA_SUBJECT_QUALIFIER)
                .build();
    }

    private static FieldTypeProtos.StringField buildOafEntityResultMetadataDateofacceptance(CharSequence earliestPublnDate) {
        return FieldTypeProtos.StringField.newBuilder()
                .setValue(earliestPublnDate.toString())
                .build();
    }

    private static FieldTypeProtos.StructuredProperty buildOafEntityResultMetadataRelevantdate(CharSequence applnFilingDate) {
        return FieldTypeProtos.StructuredProperty.newBuilder()
                .setValue(applnFilingDate.toString())
                .setQualifier(OAF_ENTITY_RESULT_METADATA_RELEVANTDATE_QUALIFIER)
                .build();
    }

    private static List<FieldTypeProtos.Author> buildOafEntityResultMetadataAuthors(List<HolderCountry> holderCountries) {
        List<CharSequence> personNames = holderCountries.stream()
                .map(HolderCountry::getPersonName)
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList());
        return ListUtils.zipWithIndex(personNames).stream()
                .map(pair -> {
                    Integer rank = pair.getLeft() + 1;
                    CharSequence personName = pair.getRight();
                    return buildOafEntityResultMetadataAuthor(personName, rank);
                })
                .collect(Collectors.toList());
    }

    private static FieldTypeProtos.Author buildOafEntityResultMetadataAuthor(CharSequence personName, Integer rank) {
        return FieldTypeProtos.Author.newBuilder()
                .setFullname(personName.toString())
                .setRank(rank)
                .build();
    }

    private static List<FieldTypeProtos.Qualifier> buildOafEntityResultMetadataCountries(List<HolderCountry> holderCountries) {
        return holderCountries.stream()
                .map(HolderCountry::getPersonCtryCode)
                .filter(StringUtils::isNotBlank)
                .distinct()
                .sorted()
                .map(PatentExporterJob::buildOafEntityResultMetadataCountry)
                .collect(Collectors.toList());
    }

    private static FieldTypeProtos.Qualifier buildOafEntityResultMetadataCountry(CharSequence country) {
        return FieldTypeProtos.Qualifier.newBuilder()
                .setClassid(country.toString())
                .setClassname(country.toString())
                .setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_COUNTRIES)
                .setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_COUNTRIES)
                .build();
    }

    private static String appendMd5(String prefix, String suffix) {
        return prefix + AbstractDNetXsltFunctions.md5(suffix);
    }

    private static Tuple2<Text, Text> actionToTuple(AtomicAction atomicAction) {
        return new Tuple2<>(new Text(atomicAction.getRowKey()), new Text(atomicAction.toString()));
    }

    @Parameters(separators = "=")
    private static class JobParameters {
        @Parameter(names = "-inputDocumentToPatentPath", required = true)
        private String inputDocumentToPatentPath;

        @Parameter(names = "-inputPatentPath", required = true)
        private String inputPatentPath;

        @Parameter(names = "-relationActionSetId", required = true)
        private String relationActionSetId;

        @Parameter(names = "-entityActionSetId", required = true)
        private String entityActionSetId;

        @Parameter(names = "-trustLevelThreshold")
        private String trustLevelThreshold;

        @Parameter(names = "-patentDateOfCollection", required = true)
        private String patentDateOfCollection;

        @Parameter(names = "-patentEpoUrlRoot", required = true)
        private String patentEpoUrlRoot;

        @Parameter(names = "-outputRelationPath", required = true)
        private String outputRelationPath;

        @Parameter(names = "-outputEntityPath", required = true)
        private String outputEntityPath;

        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
    }
}
