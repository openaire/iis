package eu.dnetlib.iis.wf.export.actionmanager.sequencefile;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.export.actionmanager.module.ActionBuilderFactory;
import eu.dnetlib.iis.wf.export.actionmanager.module.ActionBuilderModule;
import eu.dnetlib.iis.wf.export.actionmanager.module.AlgorithmName;
import eu.dnetlib.iis.wf.export.actionmanager.module.TrustLevelThresholdExceededException;

/**
 * Mock implementation of action builder factory maintaining DocumentToProject records..
 * @author mhorst
 *
 */
public class MockDocumentProjectActionBuilderFactory implements ActionBuilderFactory<DocumentToProject, Relation> {


    @Override
    public ActionBuilderModule<DocumentToProject, Relation> instantiate(Configuration config) {
        return new ActionBuilderModule<DocumentToProject, Relation>() {
            
            @SuppressWarnings("unchecked")
            @Override
            public List<AtomicAction<Relation>> build(DocumentToProject object) throws TrustLevelThresholdExceededException {

                AtomicAction<Relation> action = new AtomicAction<Relation>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public String toString() {
                        return toStringRepresentation(object);
                    }
                };

                return Lists.newArrayList(action);
            }
        };
    }

    @Override
    public AlgorithmName getAlgorithName() {
        return AlgorithmName.document_referencedProjects;
    }

    /**
     * Generates string representation of input object.
     */
    public static String toStringRepresentation(DocumentToProject input) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(input.getDocumentId());
        strBuilder.append("|");
        strBuilder.append(input.getProjectId());
        strBuilder.append("|");
        strBuilder.append(input.getConfidenceLevel());
        return strBuilder.toString();
    }
    
}
