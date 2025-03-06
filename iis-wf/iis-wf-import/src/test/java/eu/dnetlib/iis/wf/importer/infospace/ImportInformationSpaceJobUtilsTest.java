package eu.dnetlib.iis.wf.importer.infospace;

import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.iis.common.schemas.IdentifierMapping;
import eu.dnetlib.iis.common.spark.TestWithSharedSparkSession;
import eu.dnetlib.iis.wf.importer.infospace.approver.DataInfoBasedApprover;
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
import static org.mockito.Mockito.*;

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
        @DisplayName("Result records produce object store ids")
        public void givenResultRecords_whenObjectStoreIdsAreProduced_thenProperIdentifierMappingsAreCreated() {
            Dataset dataset = createResultEntity(Dataset::new, "dataset-id",
                    "50|dataset-objectstore-id", "50|dataset-objectstore-id");
            JavaRDD<Dataset> sourceDataset = jsc.parallelize(Collections.singletonList(dataset));
            OtherResearchProduct orp = createResultEntity(OtherResearchProduct::new, "orp-id",
                    "50|orp-objectstore-id", "50|orp-objectstore-id");
            JavaRDD<OtherResearchProduct> sourceOtherResearchProduct = jsc.parallelize(Collections.singletonList(orp));
            Publication publication = createResultEntity(Publication::new, "publication-id",
                    "50|publication-objectstore-id", "50|publication-objectstore-id");
            JavaRDD<Publication> sourcePublication = jsc.parallelize(Collections.singletonList(publication));
            Software software = createResultEntity(Software::new, "software-id",
                    "50|software-objectstore-id", "50|software-objectstore-id");
            JavaRDD<Software> sourceSoftware = jsc.parallelize(Collections.singletonList(software));

            DataInfoBasedApprover dataInfoBasedApprover = mock(DataInfoBasedApprover.class, withSettings().serializable());
            when(dataInfoBasedApprover.approve(dataset)).thenReturn(true);
            when(dataInfoBasedApprover.approve(orp)).thenReturn(true);
            when(dataInfoBasedApprover.approve(publication)).thenReturn(true);
            when(dataInfoBasedApprover.approve(software)).thenReturn(true);

            List<IdentifierMapping> result = ImportInformationSpaceJobUtils.produceGraphIdToObjectStoreIdMapping(sourceDataset,
                    sourceOtherResearchProduct,
                    sourcePublication,
                    sourceSoftware,
                    dataInfoBasedApprover,
                    spark()).collect();

            assertThat(result.size(), equalTo(4));
            assertThat(result, hasItem(createIdentifierMapping("dataset-id", "50|dataset-objectstore-id")));
            assertThat(result, hasItem(createIdentifierMapping("orp-id", "50|orp-objectstore-id")));
            assertThat(result, hasItem(createIdentifierMapping("publication-id", "50|publication-objectstore-id")));
            assertThat(result, hasItem(createIdentifierMapping("software-id", "50|software-objectstore-id")));
        }
    }

    @Nested
    public class ApplyDedupMappingOnTopTest {

        @Test
        @DisplayName("Deduplication mapping is empty")
        public void givenEmptyDedupMapping_whenApplyDedupMappingOnTop_thenOriginalMappingIsReturned() {
            JavaRDD<IdentifierMapping> originalIdMapping = jsc.parallelize(Arrays.asList(
                    createIdentifierMapping("new1", "original1"),
                    createIdentifierMapping("new2", "original2")
            ));
            JavaRDD<IdentifierMapping> dedupMapping = jsc.emptyRDD();

            List<IdentifierMapping> result = ImportInformationSpaceJobUtils.applyDedupMappingOnTop(originalIdMapping, dedupMapping).collect();

            assertThat(result.size(), equalTo(2));
            assertThat(result, hasItem(createIdentifierMapping("new1", "original1")));
            assertThat(result, hasItem(createIdentifierMapping("new2", "original2")));
        }

        @Test
        @DisplayName("Deduplication mapping is applied")
        public void givenDedupMapping_whenApplyDedupMappingOnTop_thenMappingsAreUpdated() {
            JavaRDD<IdentifierMapping> originalIdMapping = jsc.parallelize(Arrays.asList(
                    createIdentifierMapping("new1", "original1"),
                    createIdentifierMapping("new2", "original2")
            ));
            JavaRDD<IdentifierMapping> dedupMapping = jsc.parallelize(Arrays.asList(
                    createIdentifierMapping("dedup1", "new1"),
                    createIdentifierMapping("dedup2", "new2")
            ));

            List<IdentifierMapping> result = ImportInformationSpaceJobUtils.applyDedupMappingOnTop(originalIdMapping, dedupMapping).collect();

            assertThat(result.size(), equalTo(2));
            assertThat(result, hasItem(createIdentifierMapping("dedup1", "original1")));
            assertThat(result, hasItem(createIdentifierMapping("dedup2", "original2" )));
        }

        @Test
        @DisplayName("Partial deduplication mapping is applied")
        public void givenPartialDedupMapping_whenApplyDedupMappingOnTop_thenOnlyMatchedMappingsAreUpdated() {
            JavaRDD<IdentifierMapping> originalIdMapping = jsc.parallelize(Arrays.asList(
                    createIdentifierMapping("new1", "original1"),
                    createIdentifierMapping("new2", "original2"),
                    createIdentifierMapping("new3", "original3")
            ));
            JavaRDD<IdentifierMapping> dedupMapping = jsc.parallelize(Collections.singletonList(
                    createIdentifierMapping("dedup1", "new1")
            ));

            List<IdentifierMapping> result = ImportInformationSpaceJobUtils.applyDedupMappingOnTop(originalIdMapping, dedupMapping).collect();

            assertThat(result.size(), equalTo(3));
            assertThat(result, hasItem(createIdentifierMapping("dedup1", "original1")));
            assertThat(result, hasItem(createIdentifierMapping("new2", "original2")));
            assertThat(result, hasItem(createIdentifierMapping("new3", "original3")));
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

