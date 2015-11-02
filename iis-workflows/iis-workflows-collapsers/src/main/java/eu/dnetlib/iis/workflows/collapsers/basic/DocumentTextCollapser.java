package eu.dnetlib.iis.workflows.collapsers.basic;

import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import java.util.List;
import com.google.common.collect.Lists;

/**
 * Collapses DocumentText records by concatenating their text.
 * A single object is produced.
 * 
 * @author Dominika Tkaczyk
 */
public class DocumentTextCollapser extends SimpleCollapser<DocumentText> {

    @Override
    protected List<DocumentText> collapseNonEmpty(List<DocumentText> objects) {
        StringBuilder texts = new StringBuilder();
        for (DocumentText documentText : objects) {
            texts.append(documentText.getText());
            texts.append("\n\n");
        }
        DocumentText text = DocumentText.newBuilder()
                .setId(objects.get(0).getId())
                .setText(texts.toString().trim())
                .build();
        return Lists.newArrayList(text);
    }

}
 