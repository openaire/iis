package eu.dnetlib.iis.wf.export.actionmanager.entity;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Optional;
import eu.dnetlib.actionmanager.actions.ActionFactory;
import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.data.mapreduce.util.OafDecoder;
import eu.dnetlib.data.proto.*;
import eu.dnetlib.data.transform.xml.AbstractDNetXsltFunctions;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.common.java.stream.StreamUtils;
import eu.dnetlib.iis.referenceextraction.patent.schemas.DocumentToPatent;
import eu.dnetlib.iis.referenceextraction.patent.schemas.HolderCountry;
import eu.dnetlib.iis.referenceextraction.patent.schemas.Patent;
import eu.dnetlib.iis.wf.export.actionmanager.cfg.StaticConfigurationProvider;
import eu.dnetlib.iis.wf.export.actionmanager.common.DateTimeUtils;
import eu.dnetlib.iis.wf.export.actionmanager.common.RDDUtils;
import eu.dnetlib.iis.wf.export.actionmanager.module.AlgorithmName;
import eu.dnetlib.iis.wf.export.actionmanager.module.BuilderModuleHelper;
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
    private static final String EPO_APPLN_ID = "epo_appln_id";
    private static final String IPC = "IPC";
    private static final String INFERENCE_PROVENANCE = buildInferenceProvenance();
    private static final String PATENT_DATASOURCE_OPENAIRE_ID_PREFIX = InfoSpaceConstants.ROW_PREFIX_DATASOURCE + InfoSpaceConstants.OPENAIRE_ENTITY_ID_PREFIX + InfoSpaceConstants.ID_NAMESPACE_SEPARATOR;
    private static final String PATENT_RESULT_OPENAIRE_ID_PREFIX = InfoSpaceConstants.ROW_PREFIX_RESULT + InfoSpaceConstants.OPENAIRE_ENTITY_ID_PREFIX + InfoSpaceConstants.ID_NAMESPACE_SEPARATOR;
    private static final String PATENT_ID_PREFIX_EPO = buildRowPrefixDatasourceOpenaireEntityIdPrefixEpo();
    private static final ResultResultProtos.ResultResult OAFREL_RESULTRESULT = buildOafRelResultResult();
    private static final FieldTypeProtos.KeyValue OAF_ENTITY_COLLECTEDFROM = buildOafEntityCollectedfrom();
    private static final FieldTypeProtos.Qualifier OAF_ENTITY_RESULT_METADATA_RESULTTYPE = buildOafEntityResultMetadataResulttype();
    private static final ResultProtos.Result.Instance OAF_ENTITY_RESULT_INSTANCE = buildOafEntityResultInstance();
    private static final FieldTypeProtos.Qualifier OAF_ENTITY_PID_QUALIFIER = buildOafEntityPidQualifier();
    private static final FieldTypeProtos.Qualifier OAF_ENTITY_RESULT_METADATA_TITLE_QUALIFIER = buildOafEntityResultMetadataTitleQualifier();
    private static final FieldTypeProtos.Qualifier OAF_ENTITY_RESULT_METADATA_SUBJECT_QUALIFIER = buildOafEntityResultMetadataSubjectQualifier();
    private static final FieldTypeProtos.Qualifier OAF_ENTITY_RESULT_METADATA_RELEVANTDATE_QUALIFIER = buildOafEntityResultMetadataRelevantdateQualifier();

    private static final SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static final int numberOfOutputFiles = 10;
    private static final ActionFactory actionFactory = new ActionFactory();
    private static final PatentExportCounterReporter counterReporter = new PatentExportCounterReporter();

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
            Float trustLevelThreshold = ConfidenceLevelUtils.evaluateConfidenceLevelThreshold(params.trustLevelThreshold);

            JavaRDD<DocumentToPatent> documentToPatents = avroLoader
                    .loadJavaRDD(sc, params.inputDocumentToPatentPath, DocumentToPatent.class)
                    .cache();
            JavaRDD<Patent> patents = avroLoader
                    .loadJavaRDD(sc, params.inputPatentPath, Patent.class);

            JavaRDD<DocumentToPatent> documentToPatentsToExport =
                    documentToPatentsToExport(documentToPatents, trustLevelThreshold)
                            .cache();

            JavaPairRDD<Text, Text> relationsToExport =
                    relationsToExport(documentToPatentsToExport, params.relationActionSetId)
                            .cache();
            RDDUtils.saveTextPairRDD(relationsToExport, numberOfOutputFiles, params.outputRelationPath, configuration);

            JavaPairRDD<Text, Text> entitiesToExport =
                    entitiesToExport(documentToPatentsToExport, patents, params.entityActionSetId)
                            .cache();
            RDDUtils.saveTextPairRDD(entitiesToExport, numberOfOutputFiles, params.outputEntityPath, configuration);

            counterReporter.report(sc, relationsToExport, entitiesToExport, params.outputReportPath);
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

    private static FieldTypeProtos.KeyValue buildOafEntityCollectedfrom() {
        return FieldTypeProtos.KeyValue.newBuilder()
                .setKey(PATENT_ID_PREFIX_EPO)
                .setValue(EPO)
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

    private static ResultProtos.Result.Instance buildOafEntityResultInstance() {
        return ResultProtos.Result.Instance.newBuilder()
                .setInstancetype(buildOafEntityResultInstanceInstancetype())
                .setHostedby(buildOafEntityResultInstanceHostedby())
                .setCollectedfrom(buildOafEntityResultInstanceCollectedfrom())
                .build();
    }

    private static FieldTypeProtos.Qualifier buildOafEntityResultInstanceInstancetype() {
        return FieldTypeProtos.Qualifier.newBuilder()
                .setClassid(InfoSpaceConstants.SEMANTIC_CLASS_INSTANCE_TYPE_PATENT)
                .setClassname(InfoSpaceConstants.SEMANTIC_CLASS_PATENT)
                .setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PUBLICATION_RESOURCE)
                .setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PUBLICATION_RESOURCE)
                .build();
    }

    private static FieldTypeProtos.KeyValue buildOafEntityResultInstanceHostedby() {
        return FieldTypeProtos.KeyValue.newBuilder()
                .setKey(PATENT_ID_PREFIX_EPO)
                .setValue(EPO)
                .build();
    }

    private static FieldTypeProtos.KeyValue buildOafEntityResultInstanceCollectedfrom() {
        return FieldTypeProtos.KeyValue.newBuilder()
                .setKey(PATENT_ID_PREFIX_EPO)
                .setValue(EPO)
                .build();
    }

    private static FieldTypeProtos.Qualifier buildOafEntityPidQualifier() {
        return FieldTypeProtos.Qualifier.newBuilder()
                .setClassid(EPO_APPLN_ID)
                .setClassname(EPO_APPLN_ID)
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
                .setClassname(IPC)
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

    private static JavaPairRDD<Text, Text> relationsToExport(JavaRDD<DocumentToPatent> documentToPatentsToExport, String relationActionSetId) {
        return documentToPatentsToExport
                .flatMap(x -> buildRelationActions(x, relationActionSetId))
                .mapToPair(PatentExporterJob::actionToTuple);
    }

    private static List<AtomicAction> buildRelationActions(DocumentToPatent documentToPatent, String relationActionSetId) {
        OafProtos.Oaf.Builder builder = builderForRelationOaf(documentToPatent);

        OafProtos.Oaf forwardOaf = builder.build();
        AtomicAction forwardAction = actionFactory.createAtomicAction(
                relationActionSetId,
                StaticConfigurationProvider.AGENT_DEFAULT,
                documentId(documentToPatent.getDocumentId()),
                OafDecoder.decode(forwardOaf).getCFQ(),
                patentId(documentToPatent.getPatentId()),
                forwardOaf.toByteArray());

        OafProtos.Oaf reverseOaf = BuilderModuleHelper.invertBidirectionalRelationAndBuild(builder);
        AtomicAction reverseAction = actionFactory.createAtomicAction(
                relationActionSetId,
                StaticConfigurationProvider.AGENT_DEFAULT,
                patentId(documentToPatent.getPatentId()),
                OafDecoder.decode(reverseOaf).getCFQ(),
                documentId(documentToPatent.getDocumentId()),
                reverseOaf.toByteArray());

        return Arrays.asList(forwardAction, reverseAction);
    }

    private static OafProtos.Oaf.Builder builderForRelationOaf(DocumentToPatent documentToPatent) {
        return OafProtos.Oaf.newBuilder()
                .setKind(KindProtos.Kind.relation)
                .setRel(buildOafRel(documentToPatent))
                .setDataInfo(BuilderModuleHelper.buildInferenceForConfidenceLevel(documentToPatent.getConfidenceLevel(), INFERENCE_PROVENANCE))
                .setLastupdatetimestamp(System.currentTimeMillis());
    }

    private static OafProtos.OafRel buildOafRel(DocumentToPatent documentToPatent) {
        return OafProtos.OafRel.newBuilder()
                .setRelType(RelTypeProtos.RelType.resultResult)
                .setSubRelType(RelTypeProtos.SubRelType.relationship)
                .setRelClass(ResultResultProtos.ResultResult.Relationship.RelName.isRelatedTo.name())
                .setSource(documentId(documentToPatent.getDocumentId()))
                .setTarget(patentId(documentToPatent.getPatentId()))
                .setChild(false)
                .setResultResult(OAFREL_RESULTRESULT)
                .build();
    }

    private static JavaPairRDD<Text, Text> entitiesToExport(JavaRDD<DocumentToPatent> documentToPatentsToExport, JavaRDD<Patent> patents,
                                                            String entityActionSetId) {
        JavaPairRDD<CharSequence, Patent> patentsById = patents
                .mapToPair(x -> new Tuple2<>(x.getApplnId(), x));

        return documentToPatentsToExport
                .map(DocumentToPatent::getPatentId)
                .distinct()
                .mapToPair(x -> new Tuple2<>(x, 1))
                .leftOuterJoin(patentsById)
                .values()
                .map(x -> x._2)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(x -> buildEntityAction(x, entityActionSetId))
                .mapToPair(PatentExporterJob::actionToTuple);
    }

    private static AtomicAction buildEntityAction(Patent patent, String entityActionSetId) {
        OafProtos.Oaf oaf = buildEntityOaf(patent);
        return actionFactory.createAtomicAction(
                entityActionSetId,
                StaticConfigurationProvider.AGENT_DEFAULT,
                patentId(patent.getApplnId()),
                OafDecoder.decode(oaf).getCFQ(),
                InfoSpaceConstants.QUALIFIER_BODY_STRING,
                oaf.toByteArray()
        );
    }

    private static OafProtos.Oaf buildEntityOaf(Patent patent) {
        return OafProtos.Oaf.newBuilder()
                .setKind(KindProtos.Kind.entity)
                .setEntity(buildOafEntity(patent))
                .setLastupdatetimestamp(System.currentTimeMillis())
                .build();
    }

    private static OafProtos.OafEntity buildOafEntity(Patent patent) {
        String now = DateTimeUtils.format(LocalDateTime.now());
        return OafProtos.OafEntity.newBuilder()
                .setType(TypeProtos.Type.result)
                .setId(patentId(patent.getApplnId()))
                .addCollectedfrom(OAF_ENTITY_COLLECTEDFROM)
                .addPid(buildOafEntityPid(patent))
                .setDateofcollection(now)
                .setDateoftransformation(now)
                .setResult(buildOafEntityResult(patent))
                .build();
    }

    private static FieldTypeProtos.StructuredProperty buildOafEntityPid(Patent patent) {
        return FieldTypeProtos.StructuredProperty.newBuilder()
                .setValue(patent.getApplnId().toString())
                .setQualifier(OAF_ENTITY_PID_QUALIFIER)
                .build();
    }


    private static ResultProtos.Result buildOafEntityResult(Patent patent) {
        return ResultProtos.Result.newBuilder()
                .setMetadata(buildOafEntityResultMetadata(patent))
                .addInstance(OAF_ENTITY_RESULT_INSTANCE)
                .build();
    }

    private static ResultProtos.Result.Metadata buildOafEntityResultMetadata(Patent patent) {
        ResultProtos.Result.Metadata.Builder builder = ResultProtos.Result.Metadata.newBuilder();

        if (Objects.nonNull(patent.getApplnTitle())) {
            builder.addTitle(buildOafEntityResultMetadataTitle(patent));
        }

        if (Objects.nonNull(patent.getApplnAbstract())) {
            builder.addDescription(buildOafEntityResultMetadataDescription(patent));
        }

        return builder
                .addAllSubject(buildOafEntityResultMetadataSubjects(patent))
                .setDateofacceptance(buildOafEntityResultMetadataDateofacceptance(patent))
                .addRelevantdate(buildOafEntityResultMetadataRelevantdate(patent))
                .addAllAuthor(buildOafEntityResultMetadataAuthors(patent))
                .addAllCountry(buildOafEntityResultMetadataCountries(patent))
                .setResulttype(OAF_ENTITY_RESULT_METADATA_RESULTTYPE)
                .build();
    }

    private static FieldTypeProtos.StructuredProperty buildOafEntityResultMetadataTitle(Patent patent) {
        return FieldTypeProtos.StructuredProperty.newBuilder()
                .setValue(patent.getApplnTitle().toString())
                .setQualifier(OAF_ENTITY_RESULT_METADATA_TITLE_QUALIFIER)
                .build();
    }

    private static FieldTypeProtos.StringField buildOafEntityResultMetadataDescription(Patent patent) {
        return FieldTypeProtos.StringField.newBuilder()
                .setValue(patent.getApplnAbstract().toString())
                .build();
    }

    private static String documentId(CharSequence documentId) {
        return documentId.toString();
    }

    private static String patentId(CharSequence patentId) {
        return appendMd5(PATENT_RESULT_OPENAIRE_ID_PREFIX, patentId.toString());
    }

    private static List<FieldTypeProtos.StructuredProperty> buildOafEntityResultMetadataSubjects(Patent patent) {
        return patent.getIpcClassSymbol().stream()
                .map(PatentExporterJob::buildOafEntityResultMetadataSubject)
                .collect(Collectors.toList());
    }

    private static FieldTypeProtos.StructuredProperty buildOafEntityResultMetadataSubject(CharSequence ipcClassSymbol) {
        return FieldTypeProtos.StructuredProperty.newBuilder()
                .setValue(ipcClassSymbol.toString())
                .setQualifier(OAF_ENTITY_RESULT_METADATA_SUBJECT_QUALIFIER)
                .build();
    }

    private static FieldTypeProtos.StringField buildOafEntityResultMetadataDateofacceptance(Patent patent) {
        return FieldTypeProtos.StringField.newBuilder()
                .setValue(patent.getEarliestPublnDate().toString())
                .build();
    }

    private static FieldTypeProtos.StructuredProperty buildOafEntityResultMetadataRelevantdate(Patent patent) {
        return FieldTypeProtos.StructuredProperty.newBuilder()
                .setValue(patent.getApplnFilingDate().toString())
                .setQualifier(OAF_ENTITY_RESULT_METADATA_RELEVANTDATE_QUALIFIER)
                .build();
    }

    private static List<FieldTypeProtos.Author> buildOafEntityResultMetadataAuthors(Patent patent) {
        return filterHolderCountry(patent.getHolderCountry()).stream()
                .map(PatentExporterJob::buildOafEntityResultMetadataAuthor)
                .collect(Collectors.toList());
    }

    private static List<HolderCountry> filterHolderCountry(List<HolderCountry> holderCountries) {
        return holderCountries.stream()
                .skip(1)
                .collect(Collectors.toList());
    }

    private static FieldTypeProtos.Author buildOafEntityResultMetadataAuthor(HolderCountry holderCountry) {
        return FieldTypeProtos.Author.newBuilder()
                .setFullname(holderCountry.getPersonName().toString())
                .setRank(0)
                .build();
    }

    private static List<FieldTypeProtos.Qualifier> buildOafEntityResultMetadataCountries(Patent patent) {
        return patent.getHolderCountry().stream()
                .map(z -> z.getPersonCtryCode().toString())
                .distinct()
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

        @Parameter(names = "-outputRelationPath", required = true)
        private String outputRelationPath;

        @Parameter(names = "-outputEntityPath", required = true)
        private String outputEntityPath;

        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
    }
}
