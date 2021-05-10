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
import java.util.Collections;
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
    public class MapToObjectStoreIdTest {

        @Test
        @DisplayName("Result record ids are mapped to object store ids")
        public void givenIdentifierMappingsAndResultRecords_whenMappedToObjectStoreIds_thenProperIdentifierMappingsAreSubstituted() {
            List<IdentifierMapping> identifierMappings = Arrays.asList(
                    createIdentifierMapping("dataset-new-id-1", "dataset-original-id-1"),
                    createIdentifierMapping("organization-new-id-1", "organization-original-id-1"),
                    createIdentifierMapping("orp-new-id-1", "orp-original-id-1"),
                    createIdentifierMapping("project-new-id-1", "project-original-id-1"),
                    createIdentifierMapping("publication-new-id-1", "publication-original-id-1"),
                    createIdentifierMapping("software-new-id-1", "software-original-id-1"));
            JavaRDD<IdentifierMapping> dedupRelation = jsc.parallelize(identifierMappings);

            JavaRDD<Dataset> sourceDataset = jsc.parallelize(
                    Collections.singletonList(createResultEntity(Dataset::new, "dataset-original-id-1",
                            "dataset-objectstore-id-1", "dataset-objectstore-id-1")));
            JavaRDD<OtherResearchProduct> sourceOtherResearchProduct = jsc.parallelize(
                    Collections.singletonList(createResultEntity(OtherResearchProduct::new, "orp-original-id-1",
                            "orp-objectstore-id-1", "orp-objectstore-id-1")));
            JavaRDD<Publication> sourcePublication = jsc.parallelize(
                    Collections.singletonList(createResultEntity(Publication::new, "publication-original-id-1",
                            "publication-objectstore-id-1", "publication-objectstore-id-1")));
            JavaRDD<Software> sourceSoftware = jsc.parallelize(
                    Collections.singletonList(createResultEntity(Software::new, "software-original-id-1",
                            "software-objectstore-id-1", "software-objectstore-id-1")));

            List<IdentifierMapping> result = ImportInformationSpaceJobUtils.mapToObjectStoreId(dedupRelation,
                    sourceDataset,
                    sourceOtherResearchProduct,
                    sourcePublication,
                    sourceSoftware,
                    spark()).collect();

            assertThat(result.size(), equalTo(identifierMappings.size()));
            assertThat(result, hasItem(createIdentifierMapping("dataset-new-id-1", "dataset-objectstore-id-1")));
            assertThat(result, hasItem(createIdentifierMapping("organization-new-id-1", "organization-original-id-1")));
            assertThat(result, hasItem(createIdentifierMapping("orp-new-id-1", "orp-objectstore-id-1")));
            assertThat(result, hasItem(createIdentifierMapping("project-new-id-1", "project-original-id-1")));
            assertThat(result, hasItem(createIdentifierMapping("publication-new-id-1", "publication-objectstore-id-1")));
            assertThat(result, hasItem(createIdentifierMapping("software-new-id-1", "software-objectstore-id-1")));
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