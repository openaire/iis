package eu.dnetlib.iis.wf.importer.infospace;

import eu.dnetlib.iis.common.schemas.IdentifierMapping;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameSupport;
import eu.dnetlib.iis.common.spark.avro.AvroDatasetSupport;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

/**
 * Common utilities used in {@link ImportInformationSpaceJob}.
 */
public class ImportInformationSpaceJobUtils {

    private ImportInformationSpaceJobUtils() {

    }

    /**
     * Substitutes original ids with object store ids for records that extend {@link eu.dnetlib.dhp.schema.oaf.Result}.
     *
     * @param dedupRelation              {@link IdentifierMapping} RDD containing mappings from deduplicated ids to original ids.
     * @param sourceDataset              {@link eu.dnetlib.dhp.schema.oaf.Dataset} RDD.
     * @param sourceOtherResearchProduct {@link eu.dnetlib.dhp.schema.oaf.OtherResearchProduct} RDD.
     * @param sourcePublication          {@link eu.dnetlib.dhp.schema.oaf.Publication} RDD.
     * @param sourceSoftware             {@link eu.dnetlib.dhp.schema.oaf.Software} RDD.
     * @param spark                      Instance of SparkSession.
     * @return An RDD of {@link IdentifierMapping} with {@link eu.dnetlib.dhp.schema.oaf.Result} records original ids substituted.
     */
    public static JavaRDD<IdentifierMapping> mapToObjectStoreId(JavaRDD<IdentifierMapping> dedupRelation,
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

        org.apache.spark.sql.Dataset<Row> resultIdMapDF = sourceDatasetDS.select(col("id"), explode(col("originalId")).as("oid"))
                .union(sourceOtherResearchProductDS.select(col("id"), explode(col("originalId")).as("oid")))
                .union(sourcePublicationDS.select(col("id"), explode(col("originalId")).as("oid")))
                .union(sourceSoftwareDS.select(col("id"), explode(col("originalId")).as("oid")))
                .distinct();

        org.apache.spark.sql.Dataset<Row> dedupRelationDF = new AvroDatasetSupport(spark).toDF(
                spark.createDataset(dedupRelation.rdd(), Encoders.kryo(IdentifierMapping.class)), IdentifierMapping.SCHEMA$);

        Dataset<Row> dedupRelationWithOidDF = dedupRelationDF
                .join(resultIdMapDF, col("originalId").equalTo(col("id")), "left_outer")
                .select(col("newId"), when(col("oid").isNotNull(), col("oid"))
                        .otherwise(col("originalId")).as("originalId"));

        return new AvroDataFrameSupport(spark).toDS(dedupRelationWithOidDF, IdentifierMapping.class).toJavaRDD();
    }
}
