package eu.dnetlib.iis.wf.importer.infospace;

import eu.dnetlib.iis.common.schemas.IdentifierMapping;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameSupport;
import eu.dnetlib.iis.common.spark.avro.AvroDatasetSupport;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

/**
 * Common utilities used in {@link ImportInformationSpaceJob}.
 */
public class ImportInformationSpaceJobUtils {

    private ImportInformationSpaceJobUtils() {

    }

    /**
     * Produces a mapping from graph id to object store id by joining together a mapping from deduplicated ids into
     * original ids and a mapping for records that extend {@link eu.dnetlib.dhp.schema.oaf.Result} type.
     * <p>
     * For {@link eu.dnetlib.dhp.schema.oaf.Result} records the object store id is build from
     * {@link eu.dnetlib.dhp.schema.oaf.Result#originalId} field. For other entities the object store id is a
     * pre-deduplication id.
     * <p>
     * NOTE: this implementation servers as a temporary solution and should be unnecessary when the graph and object store
     * will be using the same ids.
     *
     * @param dedupRelation              {@link IdentifierMapping} RDD containing mappings from deduplicated ids to original ids.
     * @param sourceDataset              {@link eu.dnetlib.dhp.schema.oaf.Dataset} RDD.
     * @param sourceOtherResearchProduct {@link eu.dnetlib.dhp.schema.oaf.OtherResearchProduct} RDD.
     * @param sourcePublication          {@link eu.dnetlib.dhp.schema.oaf.Publication} RDD.
     * @param sourceSoftware             {@link eu.dnetlib.dhp.schema.oaf.Software} RDD.
     * @param spark                      Instance of SparkSession.
     * @return An RDD of {@link IdentifierMapping} with object store ids.
     */
    public static JavaRDD<IdentifierMapping> produceObjectStoreId(JavaRDD<IdentifierMapping> dedupRelation,
                                                                  JavaRDD<eu.dnetlib.dhp.schema.oaf.Dataset> sourceDataset,
                                                                  JavaRDD<eu.dnetlib.dhp.schema.oaf.OtherResearchProduct> sourceOtherResearchProduct,
                                                                  JavaRDD<eu.dnetlib.dhp.schema.oaf.Publication> sourcePublication,
                                                                  JavaRDD<eu.dnetlib.dhp.schema.oaf.Software> sourceSoftware,
                                                                  SparkSession spark) {
        Dataset<eu.dnetlib.dhp.schema.oaf.Dataset> sourceDatasetDS = spark.createDataset(
                sourceDataset.rdd(), Encoders.bean(eu.dnetlib.dhp.schema.oaf.Dataset.class));
        Dataset<eu.dnetlib.dhp.schema.oaf.OtherResearchProduct> sourceOtherResearchProductDS = spark.createDataset(
                sourceOtherResearchProduct.rdd(), Encoders.bean(eu.dnetlib.dhp.schema.oaf.OtherResearchProduct.class));
        Dataset<eu.dnetlib.dhp.schema.oaf.Publication> sourcePublicationDS = spark.createDataset(
                sourcePublication.rdd(), Encoders.bean(eu.dnetlib.dhp.schema.oaf.Publication.class));
        Dataset<eu.dnetlib.dhp.schema.oaf.Software> sourceSoftwareDS = spark.createDataset(
                sourceSoftware.rdd(), Encoders.bean(eu.dnetlib.dhp.schema.oaf.Software.class));

        Dataset<Row> resultIdMapDF = sourceDatasetDS.select(col("id"), explode(col("originalId")).as("oid"))
                .union(sourceOtherResearchProductDS.select(col("id"), explode(col("originalId")).as("oid")))
                .union(sourcePublicationDS.select(col("id"), explode(col("originalId")).as("oid")))
                .union(sourceSoftwareDS.select(col("id"), explode(col("originalId")).as("oid")))
                .where(not(col("id").like("%dedup%")))
                .where(col("oid").like("50|%"))
                .distinct();

        Dataset<Row> dedupRelationDF = new AvroDatasetSupport(spark).toDF(
                spark.createDataset(dedupRelation.rdd(), Encoders.kryo(IdentifierMapping.class)), IdentifierMapping.SCHEMA$);

        Column dedupMappingNotMatched = col("newId").isNull().and(col("id").isNotNull());
        Column resultMappingNotMatched = col("newId").isNotNull().and(col("id").isNull());

        Dataset<Row> identifierMappingDF = dedupRelationDF
                .join(resultIdMapDF, col("originalId").equalTo(col("id")), "full_outer")
                .select(
                        when(dedupMappingNotMatched, col("id"))
                                .when(resultMappingNotMatched, col("newId"))
                                .otherwise(col("newId")).as("newId"),
                        when(dedupMappingNotMatched, col("oid"))
                                .when(resultMappingNotMatched, col("originalId"))
                                .otherwise(col("oid")).as("originalId")
                );

        return new AvroDataFrameSupport(spark).toDS(identifierMappingDF, IdentifierMapping.class).toJavaRDD();
    }
}
