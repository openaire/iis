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
        @DisplayName("Dedup relations and result records produce object store ids")
        public void givenDedupRelationsAndResultRecords_whenObjectStoreIdsAreProduced_thenProperIdentifierMappingsAreCreated() {
            JavaRDD<IdentifierMapping> dedupRelation = jsc.parallelize(
                    Arrays.asList(
                            createIdentifierMapping("dataset-new-id-1", "dataset-original-id-1"),
                            createIdentifierMapping("dataset-new-id-2", "dataset-original-id-2"),
                            createIdentifierMapping("organization-new-id-1", "organization-original-id-1"),
                            createIdentifierMapping("orp-new-id-1", "orp-original-id-1"),
                            createIdentifierMapping("orp-new-id-2", "orp-original-id-2"),
                            createIdentifierMapping("project-new-id-1", "project-original-id-1"),
                            createIdentifierMapping("publication-new-id-1", "publication-original-id-1"),
                            createIdentifierMapping("publication-new-id-2", "publication-original-id-2"),
                            createIdentifierMapping("software-new-id-1", "software-original-id-1"),
                            createIdentifierMapping("software-new-id-2", "software-original-id-2"))
            );

            JavaRDD<Dataset> sourceDataset = jsc.parallelize(
                    Arrays.asList(
                            createResultEntity(Dataset::new, "dataset-original-id-1",
                                    "50|dataset-objectstore-id-1", "50|dataset-objectstore-id-1"),
                            createResultEntity(Dataset::new, "dataset-new-id-3",
                                    "50|dataset-objectstore-id-3", "50|dataset-objectstore-id-3"),
                            createResultEntity(Dataset::new, "dataset-dedup-id-4",
                                    "50|dataset-objectstore-id-4", "50|dataset-objectstore-id-4")
                    ));
            JavaRDD<OtherResearchProduct> sourceOtherResearchProduct = jsc.parallelize(
                    Arrays.asList(
                            createResultEntity(OtherResearchProduct::new, "orp-original-id-1",
                                    "50|orp-objectstore-id-1", "50|orp-objectstore-id-1"),
                            createResultEntity(OtherResearchProduct::new, "orp-new-id-3",
                                    "50|orp-objectstore-id-3", "50|orp-objectstore-id-3"),
                            createResultEntity(OtherResearchProduct::new, "orp-dedup-id-4",
                                    "50|orp-objectstore-id-4", "50|orp-objectstore-id-4")
                    ));
            JavaRDD<Publication> sourcePublication = jsc.parallelize(
                    Arrays.asList(
                            createResultEntity(Publication::new, "publication-original-id-1",
                                    "50|publication-objectstore-id-1", "50|publication-objectstore-id-1"),
                            createResultEntity(Publication::new, "publication-new-id-3",
                                    "50|publication-objectstore-id-3", "50|publication-objectstore-id-3"),
                            createResultEntity(Publication::new, "publication-dedup-id-4",
                                    "50|publication-objectstore-id-4", "50|publication-objectstore-id-4")
                    ));
            JavaRDD<Software> sourceSoftware = jsc.parallelize(
                    Arrays.asList(
                            createResultEntity(Software::new, "software-original-id-1",
                                    "50|software-objectstore-id-1", "50|software-objectstore-id-1"),
                            createResultEntity(Software::new, "software-new-id-3",
                                    "50|software-objectstore-id-3", "50|software-objectstore-id-3"),
                            createResultEntity(Software::new, "software-dedup-id-4",
                                    "50|software-objectstore-id-4", "50|software-objectstore-id-4")
                    ));

            List<IdentifierMapping> result = ImportInformationSpaceJobUtils.produceObjectStoreId(dedupRelation,
                    sourceDataset,
                    sourceOtherResearchProduct,
                    sourcePublication,
                    sourceSoftware,
                    spark()).collect();

            assertThat(result.size(), equalTo(14));
            assertThat(result, hasItem(createIdentifierMapping("dataset-new-id-1", "50|dataset-objectstore-id-1")));
            assertThat(result, hasItem(createIdentifierMapping("dataset-new-id-2", "dataset-original-id-2")));
            assertThat(result, hasItem(createIdentifierMapping("dataset-new-id-3", "50|dataset-objectstore-id-3")));
            assertThat(result, hasItem(createIdentifierMapping("organization-new-id-1", "organization-original-id-1")));
            assertThat(result, hasItem(createIdentifierMapping("orp-new-id-1", "50|orp-objectstore-id-1")));
            assertThat(result, hasItem(createIdentifierMapping("orp-new-id-2", "orp-original-id-2")));
            assertThat(result, hasItem(createIdentifierMapping("orp-new-id-3", "50|orp-objectstore-id-3")));
            assertThat(result, hasItem(createIdentifierMapping("project-new-id-1", "project-original-id-1")));
            assertThat(result, hasItem(createIdentifierMapping("publication-new-id-1", "50|publication-objectstore-id-1")));
            assertThat(result, hasItem(createIdentifierMapping("publication-new-id-2", "publication-original-id-2")));
            assertThat(result, hasItem(createIdentifierMapping("publication-new-id-3", "50|publication-objectstore-id-3")));
            assertThat(result, hasItem(createIdentifierMapping("software-new-id-1", "50|software-objectstore-id-1")));
            assertThat(result, hasItem(createIdentifierMapping("software-new-id-2", "software-original-id-2")));
            assertThat(result, hasItem(createIdentifierMapping("software-new-id-3", "50|software-objectstore-id-3")));
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