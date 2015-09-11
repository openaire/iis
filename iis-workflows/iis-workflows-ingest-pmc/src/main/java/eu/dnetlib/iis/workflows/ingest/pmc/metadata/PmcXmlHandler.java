package eu.dnetlib.iis.workflows.ingest.pmc.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.jdom.Element;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import pl.edu.icm.cermine.metadata.affiliation.CRFAffiliationParser;
import eu.dnetlib.iis.common.affiliation.AffiliationBuilder;
import eu.dnetlib.iis.ingest.pmc.metadata.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.ingest.pmc.metadata.schemas.Range;
import eu.dnetlib.iis.ingest.pmc.metadata.schemas.ReferenceBasicMetadata;
import eu.dnetlib.iis.ingest.pmc.metadata.schemas.ReferenceMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.Affiliation;


/**
 * PMC XML SAX handler.
 * 
 * @author mhorst
 *
 */
public class PmcXmlHandler extends DefaultHandler {

//	front journal
	private static final String ELEM_JOURNAL_TITLE = "journal-title";
	private static final String ELEM_JOURNAL_TITLE_GROUP = "journal-title-group";
//	front article
	private static final String ELEM_ARTICLE_META = "article-meta";
	private static final String ELEM_ARTICLE_ID = "article-id";
	private static final String ELEM_AFFILIATION = "aff";
	private static final String ELEM_LABEL = "label";
	
//	back citations
	private static final String ELEM_REF_LIST = "ref-list";
	private static final String ELEM_REF = "ref";
	private static final String ELEM_PUB_ID = "pub-id";
//	back citations meta
	private static final String ELEM_ARTICLE_TITLE = "article-title";
	private static final String ELEM_SOURCE = "source";
	private static final String ELEM_YEAR = "year";
	private static final String ELEM_VOLUME = "volume";
	private static final String ELEM_ISSUE = "issue";
	private static final String ELEM_FPAGE = "fpage";
	private static final String ELEM_LPAGE = "lpage";
//	back citations author
	private static final String ELEM_NAME = "name";
	private static final String ELEM_SURNAME = "surname";
	private static final String ELEM_GIVEN_NAMES = "given-names";
//	back citations contains text child
	private static final String ELEM_CITATION = "citation";
	private static final String ELEM_ELEMENT_CITATION = "element-citation";
	private static final String ELEM_MIXED_CITATION = "mixed-citation";
//	attributes
	private static final String PUB_ID_TYPE = "pub-id-type";
	private static final String ATTR_ARTICLE_TYPE = "article-type";
	
	public static final String PUB_ID_TYPE_PMID = "pmid";
	
	private final Logger log = Logger.getLogger(this.getClass());
	
	private Stack<String> parents;
	
	private StringBuilder currentValue = new StringBuilder();
	
	private ReferenceMetadata.Builder currentRefMetaBuilder;
	
	private String currentSurname = null;
	private String currentGivenNames = null;
	
	private List<CharSequence> currentRefAuthorList;
	private StringBuffer currentReferenceText;
	private boolean currentReferenceTextExplicitlySet = false;
	private String currentReferenceIdType = null;
	
	private String currentArticleIdType = null;
	
	boolean containsTextChild = false;
	
	boolean rootElement = true;
	
	private final ExtractedDocumentMetadata.Builder builder;
	
	/**
	 * Default constructor.
	 * @param receiver
	 */
	public PmcXmlHandler(ExtractedDocumentMetadata.Builder builder) {
		super();
		this.builder = builder;
	}
	
	@Override
	public void startDocument() throws SAXException {
		this.parents = new Stack<String>();
		clearAllFields();
	}

	@Override
	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {
		if (rootElement) {
			rootElement = false;
			String articleType = attributes.getValue(ATTR_ARTICLE_TYPE);
			if (articleType!=null) {
				builder.setEntityType(articleType);	
			} else {
				builder.setEntityType("unknown");
			}
		} else if (isWithinElement(qName, ELEM_JOURNAL_TITLE, ELEM_JOURNAL_TITLE_GROUP)) {
			this.currentValue = new StringBuilder();
		} else if (isWithinElement(qName, ELEM_ARTICLE_ID, ELEM_ARTICLE_META)) {
			this.currentArticleIdType = attributes.getValue(PUB_ID_TYPE);
			this.currentValue = new StringBuilder();
		} else if (isWithinElement(qName, ELEM_FPAGE, ELEM_ARTICLE_META) ||
				isWithinElement(qName, ELEM_LPAGE, ELEM_ARTICLE_META)) {
			this.currentValue = new StringBuilder();
		} else if (hasAmongParents(qName, ELEM_AFFILIATION, this.parents, ELEM_ARTICLE_META)) {
			this.currentValue = new StringBuilder();
		} else if (hasAmongParents(qName, ELEM_ARTICLE_TITLE, this.parents, ELEM_REF, ELEM_REF_LIST) ||
				hasAmongParents(qName, ELEM_SOURCE, this.parents, ELEM_REF, ELEM_REF_LIST) ||
				hasAmongParents(qName, ELEM_YEAR, this.parents, ELEM_REF, ELEM_REF_LIST) ||
				hasAmongParents(qName, ELEM_VOLUME, this.parents, ELEM_REF, ELEM_REF_LIST) ||
				hasAmongParents(qName, ELEM_ISSUE, this.parents, ELEM_REF, ELEM_REF_LIST) ||
				hasAmongParents(qName, ELEM_FPAGE, this.parents, ELEM_REF, ELEM_REF_LIST) ||
				hasAmongParents(qName, ELEM_LPAGE, this.parents, ELEM_REF, ELEM_REF_LIST)) {
			this.currentValue = new StringBuilder();
		} else if (isWithinElement(qName, ELEM_SURNAME, ELEM_NAME) ||
				isWithinElement(qName, ELEM_GIVEN_NAMES, ELEM_NAME)) {
			this.currentValue = new StringBuilder();
		} else if (isWithinElement(qName, ELEM_PUB_ID, ELEM_CITATION) ||
				isWithinElement(qName, ELEM_PUB_ID, ELEM_ELEMENT_CITATION) ||
				isWithinElement(qName, ELEM_PUB_ID, ELEM_MIXED_CITATION)) {
			this.currentReferenceIdType = attributes.getValue(PUB_ID_TYPE);
			this.currentValue = new StringBuilder();
		} else if (isWithinElement(qName, ELEM_REF, ELEM_REF_LIST)) {
			this.currentRefMetaBuilder = ReferenceMetadata.newBuilder();
			this.currentRefAuthorList = new ArrayList<CharSequence>();
			this.currentReferenceText = new StringBuffer();
			ReferenceBasicMetadata.Builder basicMetaBuilder = ReferenceBasicMetadata.newBuilder();
			basicMetaBuilder.setExternalIds(new HashMap<CharSequence, CharSequence>());
			this.currentRefMetaBuilder.setBasicMetadata(basicMetaBuilder.build());
		}
		this.parents.push(qName);
	}

	@Override
	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		try {
		this.parents.pop();
		if (isWithinElement(qName, ELEM_JOURNAL_TITLE, ELEM_JOURNAL_TITLE_GROUP)) {
			builder.setJournal(this.currentValue.toString().trim());
		} else if (isWithinElement(qName, ELEM_ARTICLE_ID, ELEM_ARTICLE_META) &&
				PUB_ID_TYPE_PMID.equals(this.currentArticleIdType)) {
			Map<CharSequence,CharSequence> idMapping = new HashMap<CharSequence, CharSequence>();
			idMapping.put(PUB_ID_TYPE_PMID, this.currentValue.toString().trim());
			builder.setExternalIdentifiers(idMapping);
		} else if (isWithinElement(qName, ELEM_FPAGE, ELEM_ARTICLE_META)) {
			if (builder.getPages()==null) {
				builder.setPages(Range.newBuilder().build());
			}
			builder.getPages().setStart(this.currentValue.toString().trim());
		} else if (isWithinElement(qName, ELEM_LPAGE, ELEM_ARTICLE_META)) {
			if (builder.getPages()==null) {
				builder.setPages(Range.newBuilder().build());
			}
			builder.getPages().setEnd(this.currentValue.toString().trim());
			
		} else if (hasAmongParents(qName, ELEM_AFFILIATION, this.parents, ELEM_ARTICLE_META)) {
			CRFAffiliationParser affiliationParser = new CRFAffiliationParser();
			String affStr = this.currentValue.toString();
			if (affStr.trim().length()>0) {
				try {
					Element parsedAffiliation = affiliationParser.parse(affStr);
					if (parsedAffiliation!=null) {
						if (builder.getAffiliations()==null) {
							builder.setAffiliations(new ArrayList<Affiliation>());
						}
						Affiliation aff = AffiliationBuilder.build(parsedAffiliation);
						if (aff.getRawText().length()>0) {
							builder.getAffiliations().add(aff);	
						} else {
							aff.setRawText(affStr);
						}
					}	
				} catch (IndexOutOfBoundsException e) {
//					FIXME remove this catch block when upgrading cermine version
					log.error("exception occurred when parsing affiliation: " + affStr, e);
				}
			}
		} else if (hasAmongParents(qName, ELEM_ARTICLE_TITLE, this.parents, ELEM_REF, ELEM_REF_LIST)) {
			currentRefMetaBuilder.getBasicMetadata().setTitle(this.currentValue.toString());
		} else if (hasAmongParents(qName, ELEM_SOURCE, this.parents, ELEM_REF, ELEM_REF_LIST)) {
			currentRefMetaBuilder.getBasicMetadata().setSource(this.currentValue.toString());
		} else if (hasAmongParents(qName, ELEM_YEAR, this.parents, ELEM_REF, ELEM_REF_LIST)) {
			currentRefMetaBuilder.getBasicMetadata().setYear(this.currentValue.toString());
		} else if (hasAmongParents(qName, ELEM_VOLUME, this.parents, ELEM_REF, ELEM_REF_LIST)) {
			currentRefMetaBuilder.getBasicMetadata().setVolume(this.currentValue.toString());
		} else if (hasAmongParents(qName, ELEM_ISSUE, this.parents, ELEM_REF, ELEM_REF_LIST)) {
			currentRefMetaBuilder.getBasicMetadata().setIssue(this.currentValue.toString());
		} else if (hasAmongParents(qName, ELEM_FPAGE, this.parents, ELEM_REF, ELEM_REF_LIST)) {
			if (currentRefMetaBuilder.getBasicMetadata().getPages()==null) {
				currentRefMetaBuilder.getBasicMetadata().setPages(Range.newBuilder().build());
			}
			currentRefMetaBuilder.getBasicMetadata().getPages().setStart(this.currentValue.toString());
		} else if (hasAmongParents(qName, ELEM_LPAGE, this.parents, ELEM_REF, ELEM_REF_LIST)) {
			if (currentRefMetaBuilder.getBasicMetadata().getPages()==null) {
				currentRefMetaBuilder.getBasicMetadata().setPages(Range.newBuilder().build());
			}
			currentRefMetaBuilder.getBasicMetadata().getPages().setEnd(this.currentValue.toString());
		} else if (hasAmongParents(qName, ELEM_PUB_ID, this.parents, ELEM_REF, ELEM_REF_LIST)) {
			if (this.currentReferenceIdType!=null) {
				currentRefMetaBuilder.getBasicMetadata().getExternalIds().put(
						this.currentReferenceIdType, this.currentValue.toString());	
			}
		} else if (isWithinElement(qName, ELEM_SURNAME, ELEM_NAME)) {
			this.currentSurname = this.currentValue.toString();
		} else if (isWithinElement(qName, ELEM_GIVEN_NAMES, ELEM_NAME)) {
			this.currentGivenNames = this.currentValue.toString();
		}  else if (hasAmongParents(qName, ELEM_NAME, this.parents, ELEM_REF)) {
//			in element-citation names are nested in person-group
			this.currentRefAuthorList.add(
					this.currentSurname + ", " + this.currentGivenNames);
			this.currentSurname = null;
			this.currentGivenNames = null;
		} else if (isWithinElement(qName, ELEM_CITATION, ELEM_REF) ||
				isWithinElement(qName, ELEM_ELEMENT_CITATION, ELEM_REF) ||
				isWithinElement(qName, ELEM_MIXED_CITATION, ELEM_REF)) {
			if (!this.currentRefMetaBuilder.hasText() && 
					this.currentReferenceTextExplicitlySet && 
					this.currentReferenceText!=null && this.currentReferenceText.length()>0) {
				String trimmedRefText = this.currentReferenceText.toString().trim().replaceAll(" +", " ");
				if (!trimmedRefText.isEmpty()) {
					this.currentRefMetaBuilder.setText(trimmedRefText);
				}
			}
		} else if (isWithinElement(qName, ELEM_REF, ELEM_REF_LIST)) {
			if (this.builder.getReferences()==null) {
				this.builder.setReferences(new ArrayList<ReferenceMetadata>());
			}
			this.currentRefMetaBuilder.setPosition(this.builder.getReferences().size()+1);

			if (this.currentRefAuthorList!=null && this.currentRefAuthorList.size()>0) {
				this.currentRefMetaBuilder.getBasicMetadata().setAuthors(this.currentRefAuthorList);	
			}

			if (!this.currentRefMetaBuilder.hasText()) {
				this.currentRefMetaBuilder.setText(generateReferenceRawText(
						this.currentRefMetaBuilder.getBasicMetadata()));
			}
			this.builder.getReferences().add(this.currentRefMetaBuilder.build());
//			reference fields cleanup
			this.currentRefMetaBuilder = null;
			this.currentRefAuthorList = null;
			this.currentReferenceText = null;
			this.currentReferenceTextExplicitlySet = false;
			this.currentReferenceIdType = null;
		}
		} catch (Exception e) {
//			FIXME remote this catch
			throw new RuntimeException("unexpected exception while processing doc: " + 
					builder.getId(), e);
		}
	}

	@Override
	public void endDocument() throws SAXException {
		parents.clear();
		parents = null;
	}

	@Override
	public void characters(char[] ch, int start, int length)
			throws SAXException {
		String currentElement = this.parents.pop();
		try {
//			skipping affiliation position element
			if (isWithinElement(currentElement, ELEM_LABEL, ELEM_AFFILIATION)) {
				return;
			}
			
			this.currentValue.append(ch, start, length);
//			handing reference text
			if (hasAmongParents(this.parents, ELEM_REF)) {
				if (isWithinElement(currentElement, ELEM_CITATION, ELEM_REF) ||
						isWithinElement(currentElement, ELEM_ELEMENT_CITATION, ELEM_REF) ||
						isWithinElement(currentElement, ELEM_MIXED_CITATION, ELEM_REF)) {
//					citation element contents
					char[] chunk = new char[length];
					System.arraycopy(ch, start, chunk, 0, length);
					if (containsNonWhiteCharacter(chunk)) {
						this.currentReferenceTextExplicitlySet = true;
					}
				}
				if (this.currentReferenceText.length()>0 &&
						isAlphanumeric(ch[start]) && 
						isAlphanumeric(this.currentReferenceText.charAt(
								this.currentReferenceText.length()-1))) {
//					adding missing space separator between two alphanumeric characters
					this.currentReferenceText.append(' ');
				}
				this.currentReferenceText.append(ch, start, length);
			}
		} finally {
			this.parents.push(currentElement);
		}
	}
	
	private void clearAllFields() {
		this.currentArticleIdType = null;
		this.rootElement = true;
	}
	
	static boolean isAlphanumeric(char c) {
	        return !(c < 0x30 || (c >= 0x3a && c <= 0x40) || (c > 0x5a && c <= 0x60) || c > 0x7a);
	}
	
	boolean isWithinElement(String qName,
			String expectedElement, String expectedParent) {
		return qName.equals(expectedElement) && 
				(expectedParent==null || !this.parents.isEmpty() && expectedParent.equals(this.parents.peek()));
	}
	
	public static boolean hasAmongParents(String qName,
			String expectedElement, Stack<String> parentStack, String... expectedParents) {
		if (qName.equals(expectedElement)) {
			return hasAmongParents(parentStack, expectedParents);
		} else {
			return false;	
		}
	}
	
	public static boolean hasAmongParents(Stack<String> parentStack, String... expectedParents) {
		if (expectedParents.length <= parentStack.size()) {
			int startIterationIdx = 0;
			for (String currentParent : expectedParents) {
				boolean found = false;
				for (int i=startIterationIdx; i<parentStack.size(); i++) {
//					iteration starts from the bottom while we want to check from top
					if (currentParent.equals(parentStack.get(parentStack.size()-(i+1)))) {
						startIterationIdx = i+1;
						found = true;
						break;
					}
				}
				if (!found) {
					return false;
				}
			}
			return true;
		}
		return false;
	}
	
	static boolean containsNonWhiteCharacter(char[] ch) {
		if (ch!=null && ch.length>0) {
			for (char currentCh : ch) {
				if (!Character.isWhitespace(currentCh)) {
					return true;
				}
			}
		}
		return false;
	}
	
	public static String generateReferenceRawText(ReferenceBasicMetadata refMeta) {
        String authors = refMeta.getAuthors()!=null?
        		StringUtils.join(refMeta.getAuthors(), ", "):"";
        String title = refMeta.getTitle()!=null?refMeta.getTitle().toString():null;
        String source = refMeta.getSource()!=null?refMeta.getSource().toString():null;
        String year = refMeta.getYear()!=null?refMeta.getYear().toString():null;
        String volume = refMeta.getVolume()!=null?refMeta.getVolume().toString():null;
        String issue = refMeta.getIssue()!=null?refMeta.getIssue().toString():null;
        String fpage = refMeta.getPages()!=null && refMeta.getPages().getStart()!=null
        		?refMeta.getPages().getStart().toString():null;
        String lpage = refMeta.getPages()!=null && refMeta.getPages().getEnd()!=null
        		?refMeta.getPages().getEnd().toString():null;

        StringBuilder builder = new StringBuilder();

        if (StringUtils.isNotBlank(authors)) {
            builder.append(authors);
            builder.append(". ");
        }
        if (StringUtils.isNotBlank(title)) {
            builder.append(title);
            builder.append(". ");
        }
        if (StringUtils.isNotBlank(source)) {
            builder.append(source);
            builder.append(". ");
        }
        if (StringUtils.isNotBlank(year)) {
            builder.append(year);
        }
        if (StringUtils.isNotBlank(volume)) {
            builder.append("; ");
            builder.append(volume);
        }
        if (StringUtils.isNotBlank(issue)) {
            builder.append(" (");
            builder.append(issue);
            builder.append(")");
        }
        if (StringUtils.isNotBlank(fpage)) {
            builder.append(": ");
            builder.append(fpage);
        }
        if (StringUtils.isNotBlank(lpage)) {
            builder.append("-");
            builder.append(lpage);
        }
        return builder.toString();
    }
}
