package eu.dnetlib.iis.ingest.pmc.plaintext;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.jdom.Element;
import org.jdom.Text;

import java.util.List;


/**
 * @author Dominika Tkaczyk
 * @author Michal Oniszczuk (m.oniszczuk@icm.edu.pl)
 */
public final class NlmToDocumentTextConverter {

    /**
     * Private constructor.
     */
    private NlmToDocumentTextConverter() {
    }

    public static String getDocumentText(Element source) {
        return Joiner.on("\n").skipNulls().join(
                getMetadataText(source),
                getBodyText(source),
                getReferencesText(source)
        );
    }

    public static String getMetadataText(Element source) {
        return source.getChild("front") == null ? null :
                getText(source.getChild("front"), Lists.newArrayList("journal-meta", "article-meta", "abstract"));
    }

    public static String getBodyText(Element source) {
        return source.getChild("body") == null ? null :
                getText(source.getChild("body"), Lists.newArrayList("sec", "p", "title"));
    }

    public static String getReferencesText(Element source) {
        return source.getChild("back") == null ? null :
                "References\n" + getText(source.getChild("back"), Lists.newArrayList("ref"));
    }

    /**
     * @param from                Extract text recursively from this element and its children.
     * @param insertNewlineBefore Insert newlines before these children.
     * @return Concatenated text.
     */
    private static String getText(Element from, List<String> insertNewlineBefore) {
        StringBuilder sb = new StringBuilder();

        for (Object child : from.getContent()) {
            if (child instanceof Element) {
                String childAsText = getText((Element) child, insertNewlineBefore).trim();
                if (!childAsText.isEmpty()) {
                    if (insertNewlineBefore.contains(((Element) child).getName())) {
                        sb.append("\n");
                    } else {
                        sb.append(" ");
                    }
                    sb.append(childAsText);
                }
            } else if (child instanceof Text) {
                String cont = ((Text) child).getText().trim();
                if (!cont.isEmpty()) {
                    sb.append(" ");
                    sb.append(cont);
                }
            }
        }
        return sb.toString().trim();
    }

}