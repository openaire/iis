package eu.dnetlib.iis.wf.ingest.pmc.metadata;

import static eu.dnetlib.iis.wf.ingest.pmc.metadata.PmcXmlConstants.*;
import static eu.dnetlib.iis.wf.ingest.pmc.metadata.TagHierarchyUtils.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Stack;

import org.apache.commons.lang.StringUtils;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import eu.dnetlib.iis.ingest.pmc.metadata.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.ingest.pmc.metadata.schemas.Range;
import eu.dnetlib.iis.ingest.pmc.metadata.schemas.ReferenceBasicMetadata;
import eu.dnetlib.iis.ingest.pmc.metadata.schemas.ReferenceMetadata;

/**
 * Sax xml handler of &lt;ref-list&gt; tag in pmc xml
 * 
 * @author mhorst
 * @author madryk
 *
 */
public class RefListXmlHandler extends DefaultHandler implements ParentAwareXmlHandler {
    
    private Stack<String> parents;
    
    private final ExtractedDocumentMetadata.Builder builder;
    private StringBuilder currentValue = new StringBuilder();
    
    private ReferenceMetadata.Builder currentRefMetaBuilder;
    
    private String currentSurname = null;
    private String currentGivenNames = null;
    
    private List<CharSequence> currentRefAuthorList;
    private StringBuilder currentReferenceText = new StringBuilder();
    private boolean currentReferenceTextExplicitlySet = false;
    private String currentReferenceIdType = null;
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    public RefListXmlHandler(ExtractedDocumentMetadata.Builder builder) {
        this.builder = builder;
        this.parents = new Stack<String>();
    }
    
    
    //------------------------ LOGIC --------------------------
    
    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        
        if (
                isWithinElement(qName, ELEM_SURNAME, parents, ELEM_NAME) ||
                isWithinElement(qName, ELEM_GIVEN_NAMES, parents, ELEM_NAME) ||
                hasAmongParents(qName, ELEM_ARTICLE_TITLE, this.parents, ELEM_REF) ||
                hasAmongParents(qName, ELEM_SOURCE, this.parents, ELEM_REF) ||
                hasAmongParents(qName, ELEM_YEAR, this.parents, ELEM_REF) ||
                hasAmongParents(qName, ELEM_VOLUME, this.parents, ELEM_REF) ||
                hasAmongParents(qName, ELEM_ISSUE, this.parents, ELEM_REF) ||
                hasAmongParents(qName, ELEM_FPAGE, this.parents, ELEM_REF) ||
                hasAmongParents(qName, ELEM_LPAGE, this.parents, ELEM_REF)) {
            this.currentValue.setLength(0);
        } else if (isWithinElement(qName, ELEM_PUB_ID, parents, ELEM_CITATION) ||
                isWithinElement(qName, ELEM_PUB_ID, parents, ELEM_ELEMENT_CITATION) ||
                isWithinElement(qName, ELEM_PUB_ID, parents, ELEM_MIXED_CITATION)) {
            this.currentReferenceIdType = attributes.getValue(PUB_ID_TYPE);
            this.currentValue.setLength(0);
        } else if (isElement(qName, ELEM_REF)) {
            this.currentRefMetaBuilder = ReferenceMetadata.newBuilder();
            this.currentRefAuthorList = new ArrayList<CharSequence>();
            this.currentReferenceText.setLength(0);
            ReferenceBasicMetadata.Builder basicMetaBuilder = ReferenceBasicMetadata.newBuilder();
            basicMetaBuilder.setExternalIds(new HashMap<CharSequence, CharSequence>());
            this.currentRefMetaBuilder.setBasicMetadata(basicMetaBuilder.build());
        }
        
        this.parents.push(qName);
    }
    
    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        this.parents.pop();
        
        if (hasAmongParents(qName, ELEM_ARTICLE_TITLE, this.parents, ELEM_REF)) {
            currentRefMetaBuilder.getBasicMetadata().setTitle(this.currentValue.toString());
        } else if (hasAmongParents(qName, ELEM_SOURCE, this.parents, ELEM_REF)) {
            currentRefMetaBuilder.getBasicMetadata().setSource(this.currentValue.toString());
        } else if (hasAmongParents(qName, ELEM_YEAR, this.parents, ELEM_REF)) {
            currentRefMetaBuilder.getBasicMetadata().setYear(this.currentValue.toString());
        } else if (hasAmongParents(qName, ELEM_VOLUME, this.parents, ELEM_REF)) {
            currentRefMetaBuilder.getBasicMetadata().setVolume(this.currentValue.toString());
        } else if (hasAmongParents(qName, ELEM_ISSUE, this.parents, ELEM_REF)) {
            currentRefMetaBuilder.getBasicMetadata().setIssue(this.currentValue.toString());
        } else if (hasAmongParents(qName, ELEM_FPAGE, this.parents, ELEM_REF)) {
            if (currentRefMetaBuilder.getBasicMetadata().getPages()==null) {
                currentRefMetaBuilder.getBasicMetadata().setPages(Range.newBuilder().build());
            }
            currentRefMetaBuilder.getBasicMetadata().getPages().setStart(this.currentValue.toString());
        } else if (hasAmongParents(qName, ELEM_LPAGE, this.parents, ELEM_REF)) {
            if (currentRefMetaBuilder.getBasicMetadata().getPages()==null) {
                currentRefMetaBuilder.getBasicMetadata().setPages(Range.newBuilder().build());
            }
            currentRefMetaBuilder.getBasicMetadata().getPages().setEnd(this.currentValue.toString());
        } else if (hasAmongParents(qName, ELEM_PUB_ID, this.parents, ELEM_REF)) {
            if (this.currentReferenceIdType!=null) {
                currentRefMetaBuilder.getBasicMetadata().getExternalIds().put(
                        this.currentReferenceIdType, this.currentValue.toString()); 
            }
        } else if (isWithinElement(qName, ELEM_SURNAME, parents, ELEM_NAME)) {
            this.currentSurname = this.currentValue.toString();
        } else if (isWithinElement(qName, ELEM_GIVEN_NAMES, parents, ELEM_NAME)) {
            this.currentGivenNames = this.currentValue.toString();
        }  else if (hasAmongParents(qName, ELEM_NAME, this.parents, ELEM_REF)) {
//          in element-citation names are nested in person-group
            this.currentRefAuthorList.add(
                    this.currentSurname + ", " + this.currentGivenNames);
            this.currentSurname = null;
            this.currentGivenNames = null;
        } else if (isWithinElement(qName, ELEM_CITATION, parents, ELEM_REF) ||
                isWithinElement(qName, ELEM_ELEMENT_CITATION, parents, ELEM_REF) ||
                isWithinElement(qName, ELEM_MIXED_CITATION, parents, ELEM_REF)) {
            if (!this.currentRefMetaBuilder.hasText() && 
                    this.currentReferenceTextExplicitlySet && 
                    this.currentReferenceText.length()>0) {
                String trimmedRefText = this.currentReferenceText.toString().trim().replaceAll(" +", " ");
                if (!trimmedRefText.isEmpty()) {
                    this.currentRefMetaBuilder.setText(trimmedRefText);
                }
            }
        } else if (isElement(qName, ELEM_REF)) {
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
//          reference fields cleanup
            this.currentRefMetaBuilder = null;
            this.currentRefAuthorList = null;
            this.currentReferenceText.setLength(0);
            this.currentReferenceTextExplicitlySet = false;
            this.currentReferenceIdType = null;
        }
        
    }
    
    
    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        
        String currentElement = this.parents.pop();
        try {
            
            this.currentValue.append(ch, start, length);
//          handing reference text
            if (hasAmongParents(this.parents, ELEM_REF)) {
                if (isWithinElement(currentElement, ELEM_CITATION, parents, ELEM_REF) ||
                        isWithinElement(currentElement, ELEM_ELEMENT_CITATION, parents, ELEM_REF) ||
                        isWithinElement(currentElement, ELEM_MIXED_CITATION, parents, ELEM_REF)) {
//                  citation element contents
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
//                  adding missing space separator between two alphanumeric characters
                    this.currentReferenceText.append(' ');
                }
                this.currentReferenceText.append(ch, start, length);
            }
            
        } finally {
            this.parents.push(currentElement);
        }
    }
    
    @Override
    public Stack<String> getParents() {
        return parents;
    }
    
    //------------------------ PRIVATE --------------------------
    
    private static boolean isAlphanumeric(char c) {
        return !(c < 0x30 || (c >= 0x3a && c <= 0x40) || (c > 0x5a && c <= 0x60) || c > 0x7a);
    }

    private static boolean containsNonWhiteCharacter(char[] ch) {
        if (ch!=null && ch.length>0) {
            for (char currentCh : ch) {
                if (!Character.isWhitespace(currentCh)) {
                    return true;
                }
            }
        }
        return false;
    }
    
    
    private static String generateReferenceRawText(ReferenceBasicMetadata refMeta) {
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
