package eu.dnetlib.iis.workflows.collapsers.origins;

import eu.dnetlib.iis.collapsers.schemas.DocumentTextEnvelope;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import java.util.List;
import java.util.Map;
import com.google.common.collect.Lists;

/**
 * Collapses DocumentText records coming from various origins
 * by concatenating the text. The result is one DocumentText record.
 * Concatenating is done in the order defined by the order of origins.
 * 
 * @author Dominika Tkaczyk
 */
public class DocumentTextCollapser extends OriginCollapser<DocumentTextEnvelope, DocumentText> {

    @Override
    protected List<DocumentText> collapseBetweenOrigins(Map<String, List<DocumentText>> objects) {
        CharSequence id = null;
        StringBuilder texts = new StringBuilder();
        for (String origin : origins) {
            if (objects.get(origin) != null) {
                for (DocumentText dt : objects.get(origin)) {
                    if (id == null) {
                        id = dt.getId();
                    }
                    texts.append(dt.getText());
                    texts.append("\n\n");
                }
            }
        }
        DocumentText text = DocumentText.newBuilder()
                .setId(id)
                .setText(texts.toString().trim())
                .build();
        return Lists.newArrayList(text);
    }

}
 