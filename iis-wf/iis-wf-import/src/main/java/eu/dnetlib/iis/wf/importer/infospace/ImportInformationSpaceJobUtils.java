package eu.dnetlib.iis.wf.importer.infospace;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.common.schemas.IdentifierMapping;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameSupport;
import eu.dnetlib.iis.wf.importer.infospace.approver.DataInfoBasedApprover;

/**
 * Common utilities used in {@link ImportInformationSpaceJob}.
 */
public class ImportInformationSpaceJobUtils {

    private ImportInformationSpaceJobUtils() {

    }
    
    /**
     * Merges deduplication mappings with the original to persistent identifier mappings.
     * @param originalIdMapping mapping between the original id and persistent id
     * @param dedupMapping mapping between the persistent id and deduplicated id
     * @return merged mappings
     */
    public static JavaRDD<IdentifierMapping> mergeMappings(JavaRDD<IdentifierMapping> originalIdMapping, JavaRDD<IdentifierMapping> dedupMapping) {
        return originalIdMapping.union(dedupMapping).distinct();
    }


    /**
     * Produces a mapping from graph id to object store id.
     * <p>
     * The object store ids are build for {@link eu.dnetlib.dhp.schema.oaf.Result} records as a mapping from graph ids to
     * {@link eu.dnetlib.dhp.schema.oaf.Result#originalId} fields, excluding entities that are deduplicated.
     * <p>
     * NOTE: this implementation servers as a temporary solution and should be unnecessary when the graph original ids
     * and object store ids will be the same.
     */
    public static JavaRDD<IdentifierMapping> produceGraphIdToObjectStoreIdMapping(JavaRDD<eu.dnetlib.dhp.schema.oaf.Dataset> sourceDataset,
                                                                                  JavaRDD<eu.dnetlib.dhp.schema.oaf.OtherResearchProduct> sourceOtherResearchProduct,
                                                                                  JavaRDD<eu.dnetlib.dhp.schema.oaf.Publication> sourcePublication,
                                                                                  JavaRDD<eu.dnetlib.dhp.schema.oaf.Software> sourceSoftware,
                                                                                  DataInfoBasedApprover dataInfoBasedApprover,
                                                                                  SparkSession spark) {
        Dataset<eu.dnetlib.dhp.schema.oaf.Result> sourceDatasetDS = spark.createDataset(
                sourceDataset.map(x -> (Result) x).filter(dataInfoBasedApprover::approve).rdd(),
                Encoders.bean(eu.dnetlib.dhp.schema.oaf.Result.class));
        Dataset<eu.dnetlib.dhp.schema.oaf.Result> sourceOtherResearchProductDS = spark.createDataset(
                sourceOtherResearchProduct.map(x -> (Result) x).filter(dataInfoBasedApprover::approve).rdd(),
                Encoders.bean(eu.dnetlib.dhp.schema.oaf.Result.class));
        Dataset<eu.dnetlib.dhp.schema.oaf.Result> sourcePublicationDS = spark.createDataset(
                sourcePublication.map(x -> (Result) x).filter(dataInfoBasedApprover::approve).rdd(),
                Encoders.bean(eu.dnetlib.dhp.schema.oaf.Result.class));
        Dataset<eu.dnetlib.dhp.schema.oaf.Result> sourceSoftwareDS = spark.createDataset(
                sourceSoftware.map(x -> (Result) x).filter(dataInfoBasedApprover::approve).rdd(),
                Encoders.bean(eu.dnetlib.dhp.schema.oaf.Result.class));

        Column oidIsResultType = col("oid").like(InfoSpaceConstants.ROW_PREFIX_RESULT + "%");

        Dataset<Row> resultIdMapDF = sourceDatasetDS.select(col("id"), explode(col("originalId")).as("oid"))
                .union(sourceOtherResearchProductDS.select(col("id"), explode(col("originalId")).as("oid")))
                .union(sourcePublicationDS.select(col("id"), explode(col("originalId")).as("oid")))
                .union(sourceSoftwareDS.select(col("id"), explode(col("originalId")).as("oid")))
                .where(oidIsResultType)
                .distinct();

        Dataset<Row> identifierMappingDF = resultIdMapDF
                .select(
                        col("id").as("newId"),
                        col("oid").as("originalId")
                );

        return new AvroDataFrameSupport(spark).toDS(identifierMappingDF, IdentifierMapping.class).toJavaRDD();
    }
}
