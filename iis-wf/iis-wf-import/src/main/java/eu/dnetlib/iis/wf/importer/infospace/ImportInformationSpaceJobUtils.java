package eu.dnetlib.iis.wf.importer.infospace;

import eu.dnetlib.iis.common.InfoSpaceConstants;
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
     * Produces a mapping from graph id to object store id.
     * <p>
     * The object store ids are build for {@link eu.dnetlib.dhp.schema.oaf.Result} records as a mapping from graph ids to
     * {@link eu.dnetlib.dhp.schema.oaf.Result#originalId} fields. If an entity is deduplicated the graph ids are further
     * replaced with the deduplicated ids.
     * <p>
     * NOTE: this implementation servers as a temporary solution and should be unnecessary when the graph original ids
     * and object store ids will be the same.
     *
     * @param dedupRelation              {@link IdentifierMapping} RDD containing mappings from deduplicated ids to original ids.
     * @param sourceDataset              {@link eu.dnetlib.dhp.schema.oaf.Dataset} RDD.
     * @param sourceOtherResearchProduct {@link eu.dnetlib.dhp.schema.oaf.OtherResearchProduct} RDD.
     * @param sourcePublication          {@link eu.dnetlib.dhp.schema.oaf.Publication} RDD.
     * @param sourceSoftware             {@link eu.dnetlib.dhp.schema.oaf.Software} RDD.
     * @param spark                      Instance of SparkSession.
     * @return An RDD of {@link IdentifierMapping} with object store ids.
     */
    public static JavaRDD<IdentifierMapping> produceObjectStoreId(JavaRDD<eu.dnetlib.dhp.schema.oaf.Dataset> sourceDataset,
                                                                  JavaRDD<eu.dnetlib.dhp.schema.oaf.OtherResearchProduct> sourceOtherResearchProduct,
                                                                  JavaRDD<eu.dnetlib.dhp.schema.oaf.Publication> sourcePublication,
                                                                  JavaRDD<eu.dnetlib.dhp.schema.oaf.Software> sourceSoftware,
                                                                  JavaRDD<IdentifierMapping> dedupRelation,
                                                                  SparkSession spark) {
        Dataset<eu.dnetlib.dhp.schema.oaf.Dataset> sourceDatasetDS = spark.createDataset(
                sourceDataset.rdd(), Encoders.bean(eu.dnetlib.dhp.schema.oaf.Dataset.class));
        Dataset<eu.dnetlib.dhp.schema.oaf.OtherResearchProduct> sourceOtherResearchProductDS = spark.createDataset(
                sourceOtherResearchProduct.rdd(), Encoders.bean(eu.dnetlib.dhp.schema.oaf.OtherResearchProduct.class));
        Dataset<eu.dnetlib.dhp.schema.oaf.Publication> sourcePublicationDS = spark.createDataset(
                sourcePublication.rdd(), Encoders.bean(eu.dnetlib.dhp.schema.oaf.Publication.class));
        Dataset<eu.dnetlib.dhp.schema.oaf.Software> sourceSoftwareDS = spark.createDataset(
                sourceSoftware.rdd(), Encoders.bean(eu.dnetlib.dhp.schema.oaf.Software.class));

        Column idIsNotDedup = not(col("id").like("%dedup%"));
        Column idIsResultType = col("oid").like(InfoSpaceConstants.ROW_PREFIX_RESULT + "%");

        Dataset<Row> resultIdMapDF = sourceDatasetDS.select(col("id"), explode(col("originalId")).as("oid"))
                .union(sourceOtherResearchProductDS.select(col("id"), explode(col("originalId")).as("oid")))
                .union(sourcePublicationDS.select(col("id"), explode(col("originalId")).as("oid")))
                .union(sourceSoftwareDS.select(col("id"), explode(col("originalId")).as("oid")))
                .where(idIsNotDedup)
                .where(idIsResultType)
                .distinct();

        Dataset<Row> dedupRelationDF = new AvroDatasetSupport(spark).toDF(
                spark.createDataset(dedupRelation.rdd(), Encoders.kryo(IdentifierMapping.class)), IdentifierMapping.SCHEMA$);

        Column dedupMappingNotMatched = col("newId").isNull().and(col("id").isNotNull());

        Dataset<Row> identifierMappingDF = resultIdMapDF
                .join(dedupRelationDF, col("id").equalTo(col("originalId")), "left_outer")
                .select(
                        when(dedupMappingNotMatched, col("id"))
                                .otherwise(col("newId")).as("newId"),
                        col("oid").as("originalId")
                );

        return new AvroDataFrameSupport(spark).toDS(identifierMappingDF, IdentifierMapping.class).toJavaRDD();
    }
}
