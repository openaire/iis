package eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentOrganization;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentProject;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchProjectOrganization;
import scala.Tuple2;

/**
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class DocumentOrganizationCombinerTest {

    DocumentOrganizationCombiner builder = new DocumentOrganizationCombiner();

    @Mock
    private JavaRDD<AffMatchDocumentProject> documentProjectRdd;

    @Captor
    private ArgumentCaptor<Function<AffMatchDocumentProject, String>> keyByDocumentProjectFunction;

    @Mock
    private JavaPairRDD<String, AffMatchDocumentProject> documentProjectPairedRdd;

    @Captor
    private ArgumentCaptor<Function<Tuple2<String, AffMatchDocumentProject>, Boolean>> filterDocumentProjectFunction;

    @Mock
    private JavaPairRDD<String, AffMatchDocumentProject> documentProjectPairedFilteredRdd;

    @Mock
    private JavaRDD<AffMatchProjectOrganization> projectOrganizationRdd;

    @Captor
    private ArgumentCaptor<Function<AffMatchProjectOrganization, String>> keyByProjectOrganizationFunction;

    @Mock
    private JavaPairRDD<String, AffMatchProjectOrganization> projectOrganizationPairedRdd;

    @Mock
    private JavaPairRDD<String, Tuple2<AffMatchDocumentProject, AffMatchProjectOrganization>> documentOrganizationJoinedRdd;

    @Captor
    private ArgumentCaptor<Function<Tuple2<String, Tuple2<AffMatchDocumentProject, AffMatchProjectOrganization>>, AffMatchDocumentOrganization>> createDocumentOrganizationMapFunction;

    @Mock
    private JavaRDD<AffMatchDocumentOrganization> documentOrganizationMappedRdd;

    @Mock
    private JavaRDD<AffMatchDocumentOrganization> documentOrganizationDistinctRdd;

    private final String documentId = "docId";

    private final String projectId = "projId";

    private final String organizationId = "orgId";

    private final Float confidenceLevel = 0.9f;

    // ------------------------ TESTS --------------------------
    
    @Test(expected = NullPointerException.class)
    public void build_NULL_DOCUMENT_PROJECT() {
        // execute
        builder.combine(null, projectOrganizationRdd, null);
    }

    @Test(expected = NullPointerException.class)
    public void build_NULL_PROJECT_ORGANIZATION() {
        // execute
        builder.combine(documentProjectRdd, null, null);
    }

    @Test
    public void build_WITH_NULL_THRESHOLD() throws Exception {
        // given
        stub();
        Float docProjConfidenceLevelThreshold = null;
        // execute
        JavaRDD<AffMatchDocumentOrganization> result = builder.combine(documentProjectRdd, projectOrganizationRdd,
                docProjConfidenceLevelThreshold);
        // assert
        assertResults(result, docProjConfidenceLevelThreshold);
    }

    @Test
    public void build() throws Exception {
        // given
        stub();
        Float docProjConfidenceLevelThreshold = 0.5f;
        // execute
        JavaRDD<AffMatchDocumentOrganization> result = builder.combine(documentProjectRdd, projectOrganizationRdd,
                docProjConfidenceLevelThreshold);
        // assert        
        assertResults(result, docProjConfidenceLevelThreshold);
    }

    // ------------------------ PRIVATE --------------------------

    private void stub() {
        doReturn(documentProjectPairedRdd).when(documentProjectRdd).keyBy(any());
        doReturn(documentProjectPairedFilteredRdd).when(documentProjectPairedRdd).filter(any());
        doReturn(projectOrganizationPairedRdd).when(projectOrganizationRdd).keyBy(any());
        doReturn(documentOrganizationJoinedRdd).when(documentProjectPairedFilteredRdd).join(any());
        doReturn(documentOrganizationMappedRdd).when(documentOrganizationJoinedRdd).map(any());
        when(documentOrganizationMappedRdd.distinct()).thenReturn(documentOrganizationDistinctRdd);
    }

    private void assertResults(JavaRDD<AffMatchDocumentOrganization> result, Float docProjConfidenceLevelThreshold)
            throws Exception {
        assertTrue(result == documentOrganizationDistinctRdd);
        verify(documentProjectRdd).keyBy(keyByDocumentProjectFunction.capture());
        assertKeyByDocumentProjectFunction(keyByDocumentProjectFunction.getValue());
        verify(documentProjectPairedRdd).filter(filterDocumentProjectFunction.capture());
        assertFilterDocumentProjectFunction(filterDocumentProjectFunction.getValue(),
                docProjConfidenceLevelThreshold);
        verify(projectOrganizationRdd).keyBy(keyByProjectOrganizationFunction.capture());
        assertKeyByProjectOrganizationFunction(keyByProjectOrganizationFunction.getValue());
        verify(documentProjectPairedFilteredRdd).join(projectOrganizationPairedRdd);
        verify(documentOrganizationJoinedRdd).map(createDocumentOrganizationMapFunction.capture());
        assertMapDocumentOrganizationFunction(createDocumentOrganizationMapFunction.getValue());
        verify(documentOrganizationMappedRdd).distinct();
    }

    private void assertKeyByDocumentProjectFunction(Function<AffMatchDocumentProject, String> function)
            throws Exception {
        // given
        AffMatchDocumentProject docProj = new AffMatchDocumentProject(documentId, projectId, confidenceLevel);
        // execute
        String projId = function.call(docProj);
        // assert
        assertThat(projId, equalTo(projectId));
    }

    private void assertFilterDocumentProjectFunction(Function<Tuple2<String, AffMatchDocumentProject>, Boolean> function,
            Float confidenceLevelThreshold) throws Exception {
        if (confidenceLevelThreshold != null) {
            // given
            float greaterConfidenceLevel = confidenceLevelThreshold+0.1f;
            // execute & assert
            assertTrue(function.call(new Tuple2<String, AffMatchDocumentProject>(projectId, new AffMatchDocumentProject(documentId, projectId, greaterConfidenceLevel))));
            // given
            float smallerConfidenceLevel = confidenceLevelThreshold-0.1f;
            // execute & assert
            assertFalse(function.call(new Tuple2<String, AffMatchDocumentProject>(projectId, new AffMatchDocumentProject(documentId, projectId, smallerConfidenceLevel))));
        } else {
            // execute & assert
            assertTrue(function.call(new Tuple2<String, AffMatchDocumentProject>(projectId, new AffMatchDocumentProject(documentId, projectId, 0.0001f))));
        }
    }

    private void assertKeyByProjectOrganizationFunction(
            Function<AffMatchProjectOrganization, String> function) throws Exception {
        // given
        AffMatchProjectOrganization projOrg = new AffMatchProjectOrganization(projectId, organizationId);
        // execute
        String projId = function.call(projOrg);
        // assert
        assertThat(projId, equalTo(projectId));
    }

    private void assertMapDocumentOrganizationFunction(
            Function<Tuple2<String, Tuple2<AffMatchDocumentProject, AffMatchProjectOrganization>>, AffMatchDocumentOrganization> function)
                    throws Exception {
        // execute
        AffMatchDocumentOrganization result = function
                .call(new Tuple2<String, Tuple2<AffMatchDocumentProject, AffMatchProjectOrganization>>(projectId,
                        new Tuple2<AffMatchDocumentProject, AffMatchProjectOrganization>(
                                new AffMatchDocumentProject(documentId, projectId, confidenceLevel),
                                new AffMatchProjectOrganization(projectId, organizationId))));
        // assert
        assertThat(result.getDocumentId(), equalTo(documentId));
        assertThat(result.getOrganizationId(), equalTo(organizationId));
    }
}
