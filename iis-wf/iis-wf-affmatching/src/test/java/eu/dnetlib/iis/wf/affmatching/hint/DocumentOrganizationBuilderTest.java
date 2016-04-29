package eu.dnetlib.iis.wf.affmatching.hint;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.wf.affmatching.model.DocumentOrganization;
import eu.dnetlib.iis.wf.affmatching.model.DocumentProject;
import eu.dnetlib.iis.wf.affmatching.model.ProjectOrganization;
import scala.Tuple2;

/**
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class DocumentOrganizationBuilderTest {

	DocumentOrganizationBuilder builder = new DocumentOrganizationBuilder();

	@Mock
	private JavaRDD<DocumentProject> documentProjectRdd;

	@Captor
	private ArgumentCaptor<PairFunction<DocumentProject, String, DocumentProject>> pairDocumentProjectFunctionArg;

	@Mock
	private JavaPairRDD<String, DocumentProject> documentProjectPairedRdd;

	@Captor
	private ArgumentCaptor<Function<Tuple2<String, DocumentProject>, Boolean>> filterDocumentProjectFunctionArg;

	@Mock
	private JavaPairRDD<String, DocumentProject> documentProjectPairedFilteredRdd;

	@Mock
	private JavaRDD<ProjectOrganization> projectOrganizationRdd;

	@Captor
	private ArgumentCaptor<PairFunction<ProjectOrganization, String, ProjectOrganization>> pairProjectOrganizationFunctionArg;

	@Mock
	private JavaPairRDD<String, ProjectOrganization> projectOrganizationPairedRdd;

	@Mock
	private JavaPairRDD<String, Tuple2<DocumentProject, ProjectOrganization>> documentOrganizationJoinedRdd;

	@Captor
	private ArgumentCaptor<Function<Tuple2<String, Tuple2<DocumentProject, ProjectOrganization>>, DocumentOrganization>> mapDocumentOrganizationFunctionArg;

	@Mock
	private JavaRDD<DocumentOrganization> documentOrganizationMappedRdd;

	@Mock
	private JavaRDD<DocumentOrganization> documentOrganizationDistinctRdd;

	private final String documentId = "docId";

	private final String projectId = "projId";

	private final String organizationId = "orgId";

	private final Float confidenceLevel = 0.9f;

	@Test(expected = NullPointerException.class)
	public void build_NULL_DOCUMENT_PROJECT() {
		builder.build(null, projectOrganizationRdd, null);
	}

	@Test(expected = NullPointerException.class)
	public void build_NULL_PROJECT_ORGANIZATION() {
		builder.build(documentProjectRdd, null, null);
	}

	@Test()
	public void build_WITH_NULL_THRESHOLD() throws Exception {
		stub();
		Float docProjConfidenceLevelThreshold = null;
		JavaRDD<DocumentOrganization> result = builder.build(documentProjectRdd, projectOrganizationRdd,
				docProjConfidenceLevelThreshold);
		assertResults(result, docProjConfidenceLevelThreshold);
	}

	@Test()
	public void build() throws Exception {
		stub();
		Float docProjConfidenceLevelThreshold = 0.5f;
		JavaRDD<DocumentOrganization> result = builder.build(documentProjectRdd, projectOrganizationRdd,
				docProjConfidenceLevelThreshold);
		assertResults(result, docProjConfidenceLevelThreshold);
	}

	// ------------------------ PRIVATE --------------------------

	private void stub() {
		doReturn(documentProjectPairedRdd).when(documentProjectRdd).mapToPair(any());
		doReturn(documentProjectPairedFilteredRdd).when(documentProjectPairedRdd).filter(any());
		doReturn(projectOrganizationPairedRdd).when(projectOrganizationRdd).mapToPair(any());
		doReturn(documentOrganizationJoinedRdd).when(documentProjectPairedFilteredRdd).join(any());
		doReturn(documentOrganizationMappedRdd).when(documentOrganizationJoinedRdd).map(any());
		doReturn(documentOrganizationDistinctRdd).when(documentOrganizationMappedRdd).distinct();
	}

	private void assertResults(JavaRDD<DocumentOrganization> result, Float docProjConfidenceLevelThreshold)
			throws Exception {
		assertTrue(result == documentOrganizationDistinctRdd);
		verify(documentProjectRdd).mapToPair(pairDocumentProjectFunctionArg.capture());
		assertPairDocumentProjectFunction(pairDocumentProjectFunctionArg.getValue());
		verify(documentProjectPairedRdd).filter(filterDocumentProjectFunctionArg.capture());
		assertFilterDocumentProjectFunction(filterDocumentProjectFunctionArg.getValue(),
				docProjConfidenceLevelThreshold);
		verify(projectOrganizationRdd).mapToPair(pairProjectOrganizationFunctionArg.capture());
		assertPairProjectOrganizationFunction(pairProjectOrganizationFunctionArg.getValue());
		verify(documentProjectPairedFilteredRdd).join(projectOrganizationPairedRdd);
		verify(documentOrganizationJoinedRdd).map(mapDocumentOrganizationFunctionArg.capture());
		assertMapDocumentOrganizationFunction(mapDocumentOrganizationFunctionArg.getValue());
		verify(documentOrganizationMappedRdd).distinct();
	}

	private void assertPairDocumentProjectFunction(PairFunction<DocumentProject, String, DocumentProject> function)
			throws Exception {
		DocumentProject docProj = new DocumentProject(documentId, projectId, confidenceLevel);
		Tuple2<String, DocumentProject> result = function.call(docProj);
		assertThat(result._1, equalTo(projectId));
		assertThat(result._2.getDocumentId(), equalTo(documentId));
		assertThat(result._2.getProjectId(), equalTo(projectId));
		assertThat(result._2.getConfidenceLevel(), equalTo(confidenceLevel));
	}

	private void assertFilterDocumentProjectFunction(Function<Tuple2<String, DocumentProject>, Boolean> function,
			Float confidenceLevelThreshold) throws Exception {
		assertTrue(function.call(
				new Tuple2<String, DocumentProject>(projectId, new DocumentProject(documentId, projectId, 0.6f))));
		float confidenceLevel = 0.4f;
		assertTrue((confidenceLevelThreshold == null || confidenceLevelThreshold <= confidenceLevel) == function.call(
				new Tuple2<String, DocumentProject>(projectId, new DocumentProject(documentId, projectId, confidenceLevel))));
	}

	private void assertPairProjectOrganizationFunction(
			PairFunction<ProjectOrganization, String, ProjectOrganization> function) throws Exception {
		ProjectOrganization projOrg = new ProjectOrganization(projectId, organizationId);
		Tuple2<String, ProjectOrganization> result = function.call(projOrg);
		assertThat(result._1, equalTo(projectId));
		assertThat(result._2.getProjectId(), equalTo(projectId));
		assertThat(result._2.getOrganizationId(), equalTo(organizationId));
	}

	private void assertMapDocumentOrganizationFunction(
			Function<Tuple2<String, Tuple2<DocumentProject, ProjectOrganization>>, DocumentOrganization> function)
					throws Exception {
		DocumentOrganization result = function
				.call(new Tuple2<String, Tuple2<DocumentProject, ProjectOrganization>>(projectId,
						new Tuple2<DocumentProject, ProjectOrganization>(
								new DocumentProject(documentId, projectId, confidenceLevel),
								new ProjectOrganization(projectId, organizationId))));
		assertThat(result.getDocumentId(), equalTo(documentId));
		assertThat(result.getOrganizationId(), equalTo(organizationId));
	}
}
