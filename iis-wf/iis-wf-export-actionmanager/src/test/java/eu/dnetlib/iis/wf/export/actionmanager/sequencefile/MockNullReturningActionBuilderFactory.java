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
 * Mock factory whose module returns {@code null} for a designated record, emulating builder
 * modules that signal "nothing to export" with a null list rather than an empty one.
 *
 * @author mhorst
 */
public class MockNullReturningActionBuilderFactory implements ActionBuilderFactory<DocumentToProject, Relation> {

    /** Records with this document id make {@link ActionBuilderModule#build} return {@code null}. */
    static final String NULL_RECORD_DOCUMENT_ID = "doc-null";

    @Override
    public ActionBuilderModule<DocumentToProject, Relation> instantiate(Configuration config) {
        return new ActionBuilderModule<DocumentToProject, Relation>() {

            @Override
            public List<AtomicAction<Relation>> build(DocumentToProject object)
                    throws TrustLevelThresholdExceededException {
                if (NULL_RECORD_DOCUMENT_ID.equals(object.getDocumentId().toString())) {
                    return null;
                }
                AtomicAction<Relation> action = new AtomicAction<>();
                action.setClazz(Relation.class);
                action.setPayload(MockDocumentProjectActionBuilderFactory.buildRelation(object));
                return Lists.newArrayList(action);
            }
        };
    }

    @Override
    public AlgorithmName getAlgorithName() {
        return AlgorithmName.document_referencedProjects;
    }
}
