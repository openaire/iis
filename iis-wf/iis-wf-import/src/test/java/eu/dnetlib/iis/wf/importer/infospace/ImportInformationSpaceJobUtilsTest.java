package eu.dnetlib.iis.wf.importer.infospace;

import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.iis.common.schemas.IdentifierMapping;
import eu.dnetlib.iis.common.spark.TestWithSharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class ImportInformationSpaceJobUtilsTest extends TestWithSharedSparkSession {

    private JavaSparkContext jsc;

    @BeforeEach
    public void beforeEach() {
        super.beforeEach();
        this.jsc = new JavaSparkContext(spark().sparkContext());
    }

    @Nested
    public class ProduceObjectStoreIdTest {

        @Test
        @DisplayName("Result records and dedup relations produce object store ids")
        public void givenResultRecordsAndDedupRelations_whenObjectStoreIdsAreProduced_thenProperIdentifierMappingsAreCreated() {
            JavaRDD<Dataset> sourceDataset = jsc.parallelize(
                    Arrays.asList(
                            createResultEntity(Dataset::new, "dataset-original-id-1",
                                    "50|dataset-objectstore-id-1", "50|dataset-objectstore-id-1", "dataset-objectstore-id"),
                            createResultEntity(Dataset::new, "dataset-new-id-2",
                                    "50|dataset-objectstore-id-2", "50|dataset-objectstore-id-2", "dataset-objectstore-id"),
                            createResultEntity(Dataset::new, "dataset-dedup-id",
                                    "dataset-objectstore-id", "dataset-objectstore-id")
                    ));
            JavaRDD<OtherResearchProduct> sourceOtherResearchProduct = jsc.parallelize(
                    Arrays.asList(
                            createResultEntity(OtherResearchProduct::new, "orp-original-id-1",
                                    "50|orp-objectstore-id-1", "50|orp-objectstore-id-1", "orp-objectstore-id"),
                            createResultEntity(OtherResearchProduct::new, "orp-new-id-2",
                                    "50|orp-objectstore-id-2", "50|orp-objectstore-id-2", "orp-objectstore-id"),
                            createResultEntity(OtherResearchProduct::new, "orp-dedup-id",
                                    "orp-objectstore-id", "orp-objectstore-id")
                    ));
            JavaRDD<Publication> sourcePublication = jsc.parallelize(
                    Arrays.asList(
                            createResultEntity(Publication::new, "publication-original-id-1",
                                    "50|publication-objectstore-id-1", "50|publication-objectstore-id-1", "publication-objectstore-id"),
                            createResultEntity(Publication::new, "publication-new-id-2",
                                    "50|publication-objectstore-id-2", "50|publication-objectstore-id-2", "publication-objectstore-id"),
                            createResultEntity(Publication::new, "publication-dedup-id",
                                    "publication-objectstore-id", "publication-objectstore-id")
                    ));
            JavaRDD<Software> sourceSoftware = jsc.parallelize(
                    Arrays.asList(
                            createResultEntity(Software::new, "software-original-id-1",
                                    "50|software-objectstore-id-1", "50|software-objectstore-id-1", "software-objectstore-id"),
                            createResultEntity(Software::new, "software-new-id-2",
                                    "50|software-objectstore-id-2", "50|software-objectstore-id-2", "software-objectstore-id"),
                            createResultEntity(Software::new, "software-dedup-id",
                                    "software-objectstore-id", "software-objectstore-id")
                    ));


            JavaRDD<IdentifierMapping> dedupRelation = jsc.parallelize(
                    Arrays.asList(
                            createIdentifierMapping("dataset-new-id-1", "dataset-original-id-1"),
                            createIdentifierMapping("dataset-new-id", "dataset-original-id"),
                            createIdentifierMapping("organization-new-id", "organization-original-id"),
                            createIdentifierMapping("orp-new-id-1", "orp-original-id-1"),
                            createIdentifierMapping("orp-new-id", "orp-original-id"),
                            createIdentifierMapping("project-new-id", "project-original-id"),
                            createIdentifierMapping("publication-new-id-1", "publication-original-id-1"),
                            createIdentifierMapping("publication-new-id", "publication-original-id"),
                            createIdentifierMapping("software-new-id-1", "software-original-id-1"),
                            createIdentifierMapping("software-new-id", "software-original-id"))
            );


            List<IdentifierMapping> result = ImportInformationSpaceJobUtils.produceObjectStoreId(sourceDataset,
                    sourceOtherResearchProduct,
                    sourcePublication,
                    sourceSoftware,
                    dedupRelation,
                    spark()).collect();

            assertThat(result.size(), equalTo(8));
            assertThat(result, hasItem(createIdentifierMapping("dataset-new-id-1", "50|dataset-objectstore-id-1")));
            assertThat(result, hasItem(createIdentifierMapping("dataset-new-id-2", "50|dataset-objectstore-id-2")));
            assertThat(result, hasItem(createIdentifierMapping("orp-new-id-1", "50|orp-objectstore-id-1")));
            assertThat(result, hasItem(createIdentifierMapping("orp-new-id-2", "50|orp-objectstore-id-2")));
            assertThat(result, hasItem(createIdentifierMapping("publication-new-id-1", "50|publication-objectstore-id-1")));
            assertThat(result, hasItem(createIdentifierMapping("publication-new-id-2", "50|publication-objectstore-id-2")));
            assertThat(result, hasItem(createIdentifierMapping("software-new-id-1", "50|software-objectstore-id-1")));
            assertThat(result, hasItem(createIdentifierMapping("software-new-id-2", "50|software-objectstore-id-2")));
        }
    }

    private static IdentifierMapping createIdentifierMapping(String newId, String originalId) {
        return IdentifierMapping.newBuilder().setNewId(newId).setOriginalId(originalId).build();
    }

    private static <T extends OafEntity> T createResultEntity(Supplier<T> creator, String id, String... originalId) {
        T entity = creator.get();
        entity.setId(id);
        entity.setOriginalId(Arrays.asList(originalId));
        return entity;
    }
}