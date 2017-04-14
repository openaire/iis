package eu.dnetlib.iis.wf.export.actionmanager.sequencefile;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.common.Agent;
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
public class MockDocumentProjectActionBuilderFactory implements ActionBuilderFactory<DocumentToProject> {


    @Override
    public ActionBuilderModule<DocumentToProject> instantiate(Configuration config, Agent agent, String actionSetId) {
        return new ActionBuilderModule<DocumentToProject>() {
            @Override
            public List<AtomicAction> build(DocumentToProject object) throws TrustLevelThresholdExceededException {
                
                AtomicAction action = new AtomicAction(actionSetId, agent) {
                    
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
