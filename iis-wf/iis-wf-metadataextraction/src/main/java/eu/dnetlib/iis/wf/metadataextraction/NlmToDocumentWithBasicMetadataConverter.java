package eu.dnetlib.iis.wf.metadataextraction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.util.Utf8;
import org.apache.log4j.Logger;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.xpath.XPath;

import eu.dnetlib.iis.common.hbase.HBaseConstants;
import eu.dnetlib.iis.common.importer.CermineAffiliation;
import eu.dnetlib.iis.common.importer.CermineAffiliationBuilder;
import eu.dnetlib.iis.metadataextraction.schemas.Affiliation;
import eu.dnetlib.iis.metadataextraction.schemas.Author;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.Range;
import eu.dnetlib.iis.metadataextraction.schemas.ReferenceBasicMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata;
import pl.edu.icm.cermine.bibref.model.BibEntry;
import pl.edu.icm.cermine.bibref.transformers.NLMToBibEntryConverter;
import pl.edu.icm.cermine.exception.TransformationException;

/**
 * NLM {@link Element} converter building {@link DocumentWithBasicMetadata} objects.
 * @author mhorst
 *
 */
public final class NlmToDocumentWithBasicMetadataConverter {

	public static final String EMPTY_META = "$EMPTY$";
    
    private static final Logger log = Logger.getLogger(NlmToDocumentWithBasicMetadataConverter.class);
	
	private static CermineToMetadataAffConverter cermineToMetadataAffConverter = new CermineToMetadataAffConverter();
	
	private static CermineAffiliationBuilder cermineAffiliationBuilder = new CermineAffiliationBuilder();
    
	private static NLMToBibEntryConverter bibEntryConverter = new NLMToBibEntryConverter();
	
	// ----------------------------- CONSTRUCTORS ------------------------------------
	
	/**
	 * Private constructor.
	 */
	private NlmToDocumentWithBasicMetadataConverter() {}

    // ----------------------------- LOGIC -------------------------------------------
	
	/**
     * Converts given source element to {@link DocumentWithBasicMetadata}.
     * 
     * @param id document identifier
     * @param source XML document
     * @return {@link DocumentWithBasicMetadata}, never returns null.
     * @throws TransformationException 
     */
    public static ExtractedDocumentMetadata convertFull(String id, Document source, String text) throws TransformationException {
        if (id == null) {
            throw new RuntimeException("unable to set null id");
        }
        ExtractedDocumentMetadata.Builder builder = ExtractedDocumentMetadata.newBuilder();
        builder.setId(id);
        builder.setText(text != null ? text : "");
        if (source == null) {
//          allowing returning empty extracted metadata
            return builder.build();
        }
        Element rootElement = source.getRootElement();
        convertMeta(id, rootElement, builder);
        
        Map<String, Affiliation> affiliationMap = convertAffiliations(rootElement);
        List<String> affiliationIds = new ArrayList<String>();
        
        if (!affiliationMap.isEmpty()) {
            List<Affiliation> affiliations = new ArrayList<Affiliation>();
            for (Entry<String, Affiliation> entry : affiliationMap.entrySet()) {
                affiliationIds.add(entry.getKey());
                affiliations.add(entry.getValue());
            }
            builder.setAffiliations(affiliations);
        }
        
        List<Author> authors = convertAuthors(rootElement, affiliationIds);
        if (!authors.isEmpty()) {
            builder.setAuthors(authors);
        }
        
        List<ReferenceMetadata> refs = convertReferences(rootElement);
        if (!refs.isEmpty()) {
            builder.setReferences(refs);    
        }
        return builder.build();
    }
    
    /**
     * Creates empty entry with identifier set and empty record indicator.
     * Never returns null.
     * @param id
     * @return {@link DocumentWithBasicMetadata}
     */
    public static ExtractedDocumentMetadata createEmpty(String id) {
        if (id==null) {
            throw new RuntimeException("unable to set null id");
        }
        ExtractedDocumentMetadata.Builder builder = ExtractedDocumentMetadata.newBuilder();
        builder.setId(id);
        builder.setText("");
        builder.setPublicationTypeName(EMPTY_META);
        return builder.build();
        
    }
    
    // ----------------------------- PRIVATE -----------------------------------------
	
    /**
     * Converts XML affiliations into map of avro objects.
     * Never returns null.
     * @param source main XML element
     */
    private static Map<String, Affiliation> convertAffiliations(Element source) {
        try {
            XPath xPath = XPath.newInstance("/article/front//contrib-group/aff");
            @SuppressWarnings("unchecked")
            List<Element> nodeList = xPath.selectNodes(source);
            if (nodeList == null || nodeList.isEmpty()) {
                return Collections.emptyMap();
            }
            Map<String, Affiliation> affiliations = new LinkedHashMap<String, Affiliation>();
            for (Element node : nodeList) {
                CermineAffiliation cAff = cermineAffiliationBuilder.build(node);
                affiliations.put(node.getAttributeValue("id"), cermineToMetadataAffConverter.convert(cAff));
            }
            return affiliations;
        } catch (JDOMException ex) {
            return Collections.emptyMap();
        }
	}
    
	/**
	 * Converts XML authors into avro objects augumented with affiliation positions.
	 * Never returns null.
	 * @param source main XML element
	 * @param affiliationsIds ordered affiliations
	 */
	private static List<Author> convertAuthors(Element source, List<String> affiliationsIds) {
        try {            
            XPath xPath = XPath.newInstance("/article/front//contrib-group/contrib[@contrib-type='author']");
            @SuppressWarnings("unchecked")
            List<Element> nodeList = xPath.selectNodes(source);
            if (nodeList == null || nodeList.isEmpty()) {
                return Collections.emptyList();
            }
            List<Author> authors = new ArrayList<Author>();
            for (Element element : nodeList) {
                String name = element.getChildTextNormalize("string-name");
                List<Integer> affPositions = new ArrayList<Integer>();
                List<?> xrefs = element.getChildren("xref");
                for (Object xref : xrefs) {
                    Element xrefEl = (Element) xref;
                    String xrefType = xrefEl.getAttributeValue("ref-type");
                    String xrefId = xrefEl.getAttributeValue("rid");
                    if ("aff".equals(xrefType) && xrefId != null && affiliationsIds.contains(xrefId)) {
                        affPositions.add(affiliationsIds.indexOf(xrefId));
                    }
                }
                if (affPositions.isEmpty()) {
                    affPositions = null;
                }
                Author author = Author.newBuilder()
                        .setAuthorFullName(name)
                        .setAffiliationPositions(affPositions)
                        .build();
                authors.add(author);
            }
            return authors;
        } catch (JDOMException ex) {
            return Collections.emptyList();
        }
	}
	
	/**
	 * Converts bibliographic references.
	 * @param source
	 * @return list containing all bibliographic references, never returns null
	 * @throws TransformationException 
	 */
	private static List<ReferenceMetadata> convertReferences(Element source) throws TransformationException {
		Element backElement = source.getChild("back");
		if (backElement!=null) {
			Element refListElement = backElement.getChild("ref-list");
			if (refListElement!=null) {
				@SuppressWarnings("unchecked")
				List<Element> refs = refListElement.getChildren("ref");
				int idx = 1;
				List<ReferenceMetadata> refMetas = new ArrayList<ReferenceMetadata>();
				for (Element ref : refs) {
				    ReferenceMetadata refMeta = convertReference(ref, idx);
					if (refMeta != null) {
					    refMetas.add(refMeta);
					}
					idx++;
				}
				return refMetas;
			}
		}
//		fallback
		return Collections.emptyList();
	}
	
	/**
	 * Converts single reference.
	 * @param ref reference element
	 * @param position reference position in the source list
	 * @return converted {@link ReferenceMetadata} object or null when reference did not contain valid citation
	 * @throws TransformationException
	 */
	private static ReferenceMetadata convertReference(Element ref, int position) throws TransformationException {
	    Element mixedCitation = ref.getChild("mixed-citation");
	    if (mixedCitation != null) {
            BibEntry bibEntry = bibEntryConverter.convert(mixedCitation);
            if (bibEntry!=null) {
                ReferenceMetadata.Builder refMetaBuilder = ReferenceMetadata.newBuilder();
                refMetaBuilder.setPosition(position);
                refMetaBuilder.setText(bibEntry.getText());
                refMetaBuilder.setBasicMetadata(convertBibEntry(bibEntry));
                return refMetaBuilder.build();   
            } else {
                log.warn("got null bib-entry from element " + mixedCitation.getValue());
            }
        }
	    // fallback
	    return null;
	}
	
	/**
	 * Converts {@link BibEntry} to {@link ReferenceBasicMetadata}.
	 * @param entry
	 * @return {@link ReferenceBasicMetadata}
	 */
	private static ReferenceBasicMetadata convertBibEntry(BibEntry entry) {
		if (entry!=null) {
			ReferenceBasicMetadata.Builder builder = ReferenceBasicMetadata.newBuilder();
			List<String> resultValues = entry.getAllFieldValues(BibEntry.FIELD_AUTHOR);
			if (resultValues != null && !resultValues.isEmpty()) {
				List<CharSequence> authors = new ArrayList<CharSequence>(resultValues.size());
				for (CharSequence seq : resultValues) {
					authors.add(seq);
				}
				builder.setAuthors(authors);	
			}
			String resultValue = entry.getFirstFieldValue(BibEntry.FIELD_PAGES);
			if (resultValue!=null) {
                Pattern pagesPattern = Pattern.compile("^([0-9]+)--([0-9]+)$");
                Matcher m = pagesPattern.matcher(resultValue);
                if (m.matches()) {
                    builder.setPages(Range.newBuilder().setStart(m.group(1)).setEnd(m.group(2)).build());
                } else {
                    pagesPattern = Pattern.compile("^[0-9]+$");
                    m = pagesPattern.matcher(resultValue);
                    if (m.matches()) {
                        builder.setPages(Range.newBuilder().setStart(m.group()).setEnd(m.group()).build());
                    }
                }
			}
			resultValue = entry.getFirstFieldValue(BibEntry.FIELD_JOURNAL);
			if (resultValue!=null) {
				builder.setSource(resultValue);	
			} 
			resultValue = entry.getFirstFieldValue(BibEntry.FIELD_TITLE);
			if (resultValue!=null) {
				builder.setTitle(resultValue);	
			}
			resultValue = entry.getFirstFieldValue(BibEntry.FIELD_VOLUME);
			if (resultValue!=null) {
				builder.setVolume(resultValue);	
			}
			resultValue = entry.getFirstFieldValue(BibEntry.FIELD_YEAR);
			if (resultValue!=null) {
				builder.setYear(resultValue);	
			}
			resultValue = entry.getFirstFieldValue(BibEntry.FIELD_EDITION);
			if (resultValue!=null) {
				builder.setEdition(resultValue);	
			}
			resultValue = entry.getFirstFieldValue(BibEntry.FIELD_PUBLISHER);
			if (resultValue!=null) {
				builder.setPublisher(resultValue);	
			}
			resultValue = entry.getFirstFieldValue(BibEntry.FIELD_LOCATION);
			if (resultValue!=null) {
				builder.setLocation(resultValue);	
			}
			resultValue = entry.getFirstFieldValue(BibEntry.FIELD_SERIES);
			if (resultValue!=null) {
				builder.setSeries(resultValue);	
			}
			resultValue = entry.getFirstFieldValue(BibEntry.FIELD_NUMBER);
			if (resultValue!=null) {
				builder.setIssue(resultValue);	
			}
			resultValue = entry.getFirstFieldValue(BibEntry.FIELD_URL);
			if (resultValue!=null) {
				builder.setUrl(resultValue);	
			}
			return builder.build();
		} else {
			return null;
		}
	}
	
	/**
	 * Converts metadata.
	 * @param id
	 * @param source
	 * @param builder
	 * @return DocumentBasicMetadata
	 */
	private static ExtractedDocumentMetadata.Builder convertMeta(String id, Element source,
			ExtractedDocumentMetadata.Builder docMetaBuilder) {
		Element frontElement = source.getChild("front");
		if (frontElement == null) {
		    return docMetaBuilder;
		}
		Element articleMetaElement = frontElement.getChild("article-meta");
		if (articleMetaElement!=null) {
		    docMetaBuilder = convertArticleMeta(id, articleMetaElement, docMetaBuilder);
		}
		Element journalMeta = frontElement.getChild("journal-meta");
		if (journalMeta!=null) {
		    docMetaBuilder = convertJournalMeta(id, journalMeta, docMetaBuilder);
		}
		return docMetaBuilder;
	}
	
	/**
	 * Converts article metadata element. 
	 * @param id document identifier
	 * @param articleMeta article metadata element
	 * @param docMetaBuilder document metadata avro record builder
	 */
	@SuppressWarnings("unchecked")
    private static ExtractedDocumentMetadata.Builder convertArticleMeta(String id, Element articleMeta,
            ExtractedDocumentMetadata.Builder docMetaBuilder) {
        
//      /article/front/article-meta/title-group/article-title [multiple elements]
	    Element titleGroup = articleMeta.getChild("title-group");
        if (titleGroup != null) {
            convertTitles(id, titleGroup, docMetaBuilder);
        }
        
//      /article/front/article-meta/abstract/p
        Element articleAbstract = articleMeta.getChild("abstract");
        if (articleAbstract != null) {
            Element pAbstract = articleAbstract.getChild("p");
            if (pAbstract != null) {
                docMetaBuilder.setAbstract$(pAbstract.getTextNormalize());
            }
        }
        
//      /article/front/article-meta/kwd-group/kwd [multiple elements]
        Element keywordsGroup = articleMeta.getChild("kwd-group");
        if (keywordsGroup != null) {
            convertKeywords(keywordsGroup, docMetaBuilder);
        }
        
//      /article/front/article-meta/article-id
        List<Element> articleIds = articleMeta.getChildren("article-id");
        Map<CharSequence, CharSequence> extIds = new HashMap<CharSequence, CharSequence>();
        for (Element articleIdElem : articleIds) {
            String idType = articleIdElem.getAttributeValue("pub-id-type");
            extIds.put(new Utf8(idType!=null?idType:HBaseConstants.EXTERNAL_ID_TYPE_UNKNOWN), 
                    new Utf8(articleIdElem.getTextNormalize()));
        }
        if (!extIds.isEmpty()) {
            docMetaBuilder.setExternalIdentifiers(extIds);  
        }
        
//      /article/front/article-meta/pub-date/year
        Element publicationDate = articleMeta.getChild("pub-date");
        if (publicationDate != null) {
            Element publicationDateYear = publicationDate.getChild("year");
            if (publicationDateYear != null) {
                try {
                    docMetaBuilder.setYear(Integer.valueOf(publicationDateYear.getTextNormalize()));   
                } catch (Exception e) {
                    log.error("unable to parse year, unsupported format: " + 
                            publicationDateYear.getTextNormalize() + ", document id" + id, e);
                }    
            }
        }

//      /article/front/article-meta/volume
        Element volume = articleMeta.getChild("volume");
        if (volume != null) {
            docMetaBuilder.setVolume(volume.getTextNormalize());
        }
        
//      /article/front/article-meta/issue
        Element issue = articleMeta.getChild("issue");
        if (issue != null) {
            docMetaBuilder.setIssue(issue.getTextNormalize());
        }
        
//      /article/front/article-meta/fpage
//      /article/front/article-meta/lpage
        Range.Builder pagesBuilder = Range.newBuilder();
        Element fpage = articleMeta.getChild("fpage");
        if (fpage != null) {
            pagesBuilder.setStart(fpage.getTextNormalize());
        }
        Element lpage = articleMeta.getChild("lpage");
        if (lpage != null) {
            pagesBuilder.setEnd(lpage.getTextNormalize());
        }
        if (pagesBuilder.hasStart() || pagesBuilder.hasEnd()) {
            docMetaBuilder.setPages(pagesBuilder.build());
        }
        
        return docMetaBuilder;
	}
	
	/**
	 * Converts XML title group element into avro object representation.
	 * @param id document identifier
	 * @param titleGroup title group XML element
	 * @param docMetaBuilder document metadata avro record builder
	 */
	@SuppressWarnings("unchecked")
    private static ExtractedDocumentMetadata.Builder convertTitles(String id, Element titleGroup,
            ExtractedDocumentMetadata.Builder docMetaBuilder) {
	    List<Element> titles = titleGroup.getChildren("article-title");
//      currenlty taking only first title into account!
        if (!titles.isEmpty()) {
            if (titles.size()>1) {
                log.warn("got multiple titles for document " + id + 
                        ", storing first title only");
            }
            for (Element titleElem : titles) {
//              iterating until finding not null title
                String title = titleElem.getTextNormalize();
                if (title!=null && !title.isEmpty()) {
                    docMetaBuilder.setTitle(title);
                    break;
                }   
            }
        }
        return docMetaBuilder;
	}
	
	/**
	 * Converts XML keyword group element into avro object representation.
	 * @param keywordsGroup keyword group XML element
	 * @param docMetaBuilder document metadata avro record builder
	 */
	@SuppressWarnings("unchecked")
    private static ExtractedDocumentMetadata.Builder convertKeywords(Element keywordsGroup,
            ExtractedDocumentMetadata.Builder docMetaBuilder) {
	    List<Element> keywords = keywordsGroup.getChildren("kwd");
        for (Element keywordElem : keywords) {
            String keyword = keywordElem.getTextNormalize();
            if (keyword!=null && !keyword.isEmpty()) {
                if (docMetaBuilder.getKeywords()==null) {
                    docMetaBuilder.setKeywords(new ArrayList<CharSequence>());
                }
                docMetaBuilder.getKeywords().add(keyword);
            }
        }
        return docMetaBuilder;
	}
	
	/**
	 * Converts journal metadata.
	 * @param id document identifier
	 * @param journalMeta journal metadata element
	 * @param docMetaBuilder document metadata avro record builder
	 */
	@SuppressWarnings("unchecked")
    private static ExtractedDocumentMetadata.Builder convertJournalMeta(String id, Element journalMeta,
            ExtractedDocumentMetadata.Builder docMetaBuilder) {

//      /article/front/journal-meta/journal-title-group/journal-title
	    Element titleGroup = journalMeta.getChild("journal-title-group");
        if (titleGroup != null) {
            List<Element> titles = titleGroup.getChildren("journal-title");
            if (!titles.isEmpty()) {
                docMetaBuilder.setJournal(titles.iterator().next().getTextNormalize());
                if (titles.size()>1) {
                    log.warn("got multiple journal titles, retrieving first title only. " +
                            "Document id: " + id);
                }
            }
        }
        
//      /article/front/journal-meta/publisher/publisher-name
        Element publisher = journalMeta.getChild("publisher");
        if (publisher != null) {
            Element publisherName = publisher.getChild("publisher-name");
            if (publisherName != null) {
                String pubNameText = publisherName.getTextNormalize();
                if (pubNameText != null && !pubNameText.isEmpty()) {
                    docMetaBuilder.setPublisher(pubNameText);
                }
            }
        }
        
        return docMetaBuilder;
	}
            
}
