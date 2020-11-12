package eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read;

import eu.dnetlib.iis.common.spark.TestWithSharedSparkContext;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentProject;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.of;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * @author madryk
 */
public class DocumentProjectMergerTest extends TestWithSharedSparkContext {

    private DocumentProjectMerger documentProjectMerger = new DocumentProjectMerger();

    //------------------------ TESTS --------------------------

    @Test
    public void merge() {
        // given
        JavaRDD<AffMatchDocumentProject> firstDocumentProjects = jsc().parallelize(of(
                new AffMatchDocumentProject("DOC1", "PROJ1", 1f),
                new AffMatchDocumentProject("DOC1", "PROJ2", 0.6f),
                new AffMatchDocumentProject("DOC1", "PROJ3", 0.4f),
                new AffMatchDocumentProject("DOC2", "PROJ4", 0.8f)));
        JavaRDD<AffMatchDocumentProject> secondDocumentProjects = jsc().parallelize(of(
                new AffMatchDocumentProject("DOC1", "PROJ1", 0.3f),
                new AffMatchDocumentProject("DOC1", "PROJ2", 1f),
                new AffMatchDocumentProject("DOC1", "PROJ4", 0.7f)));

        // execute
        JavaRDD<AffMatchDocumentProject> retDocumentProjects = documentProjectMerger
                .merge(firstDocumentProjects, secondDocumentProjects);

        // assert
        List<AffMatchDocumentProject> expectedDocumentProjects = of(
                new AffMatchDocumentProject("DOC1", "PROJ1", 1f), // from first rdd (higher confidence level)
                new AffMatchDocumentProject("DOC1", "PROJ2", 1f), // from second rdd (higher confidence level)
                new AffMatchDocumentProject("DOC1", "PROJ3", 0.4f),
                new AffMatchDocumentProject("DOC1", "PROJ4", 0.7f),
                new AffMatchDocumentProject("DOC2", "PROJ4", 0.8f));

        assertThat(retDocumentProjects.collect(), containsInAnyOrder(expectedDocumentProjects.toArray()));
    }
}
