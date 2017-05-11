package eu.dnetlib.iis.wf.collapsers;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

import eu.dnetlib.iis.common.schemas.Identifier;
import eu.dnetlib.iis.importer.schemas.DocumentToProject;

/**
 * @author mhorst
 *
 */
public class DummyDocumentToProjectCollapser implements RecordCollapser<DocumentToProject, Identifier> {

    @Override
    public void setup(TaskAttemptContext context) {
        // does nothing
    }

    @Override
    public List<Identifier> collapse(List<DocumentToProject> objects) {
        if (objects == null) {
            return null;
        }
        Set<CharSequence> ids = new HashSet<>();
        for (DocumentToProject currentDocProj : objects) {
            ids.add(currentDocProj.getDocumentId());
        }
        return ids.stream().map(id -> Identifier.newBuilder().setId(id).build()).collect(Collectors.toList());
    }

}
