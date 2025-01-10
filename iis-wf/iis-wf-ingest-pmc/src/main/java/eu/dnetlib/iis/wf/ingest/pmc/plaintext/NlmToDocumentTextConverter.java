package eu.dnetlib.iis.wf.ingest.pmc.plaintext;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.jdom.Element;
import org.jdom.Namespace;
import org.jdom.Text;
import org.jdom.output.XMLOutputter;

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

	public static String getDocumentText(Element source, Namespace namespace) {
		Element articleElement = getArticleElement(source, namespace);
		return Joiner.on("\n").skipNulls().join(getMetadataText(articleElement), getBodyText(articleElement),
				getReferencesText(articleElement));
	}

	/**
	 * Provides article element as root element or nested inside oai record.
	 * 
	 * @param source
	 * @param oaiNamespace
	 * @return article root element or child of oai:metadata element.
	 */
	private static Element getArticleElement(Element source, Namespace oaiNamespace) {
		Element metadata = source.getChild("metadata", oaiNamespace);
		if (metadata != null) {
			Element article = metadata.getChild("article", null);
			if (article != null) {
				return article;
			} else {
				throw new RuntimeException("unexpected NLM record contents: "
						+ "article element was not found inside OAI metadata element! Record dump: "
						+ new XMLOutputter().outputString(source));
			}
		} else {
			// source element is not wrapped with oai:metadata element
			return source;
		}
	}

	private static String getMetadataText(Element source) {
		return source.getChild("front", null) == null ? null
				: getText(source.getChild("front", null), Lists.newArrayList("journal-meta", "article-meta", "abstract"));
	}

	private static String getBodyText(Element source) {
		return source.getChild("body", null) == null ? null
				: getText(source.getChild("body", null), Lists.newArrayList("sec", "p", "title"));
	}

	private static String getReferencesText(Element source) {
		return source.getChild("back", null) == null ? null
				: "References\n" + getText(source.getChild("back", null), Lists.newArrayList("ref"));
	}

	/**
	 * @param from
	 *            Extract text recursively from this element and its children.
	 * @param insertNewlineBefore
	 *            Insert newlines before these children.
	 * @return Concatenated text.
	 */
	private static String getText(Element from, List<String> insertNewlineBefore) {
		StringBuilder sb = new StringBuilder();

		for (Object child : from.getContent()) {
			if (child instanceof Element) {
				String childAsText = getText((Element) child, insertNewlineBefore).trim();
				if (!childAsText.isEmpty()) {
					if (insertNewlineBefore.contains(((Element) child).getName())) {
						sb.append('\n');
					} else {
						sb.append(' ');
					}
					sb.append(childAsText);
				}
			} else if (child instanceof Text) {
				String cont = ((Text) child).getText().trim();
				if (!cont.isEmpty()) {
					sb.append(' ');
					sb.append(cont);
				}
			}
		}
		return sb.toString().trim();
	}

}