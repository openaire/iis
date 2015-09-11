package eu.dnetlib.iis.workflows.metadataextraction;

import java.util.ArrayList;
import java.util.HashMap;
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

import pl.edu.icm.cermine.bibref.model.BibEntry;
import pl.edu.icm.cermine.bibref.transformers.NLMElementToBibEntryConverter;
import pl.edu.icm.cermine.exception.TransformationException;
import eu.dnetlib.iis.common.affiliation.AffiliationBuilder;
import eu.dnetlib.iis.common.hbase.HBaseConstants;
import eu.dnetlib.iis.metadataextraction.schemas.Affiliation;
import eu.dnetlib.iis.metadataextraction.schemas.Author;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.Range;
import eu.dnetlib.iis.metadataextraction.schemas.ReferenceBasicMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata;

/**
 * NLM {@link Element} converter building {@link DocumentWithBasicMetadata} objects.
 * @author mhorst
 *
 */
public final class NlmToDocumentWithBasicMetadataConverter {

	private static final Logger log = Logger.getLogger(NlmToDocumentWithBasicMetadataConverter.class);
	
	/**
	 * Private constructor.
	 */
	private NlmToDocumentWithBasicMetadataConverter() {}
	
    private static Map<String, Affiliation> convertAffiliations(Element source) {
        try {
            XPath xPath = XPath.newInstance("/article/front//contrib-group/aff");
            @SuppressWarnings("unchecked")
            List<Element> nodeList = xPath.selectNodes(source);
            if (nodeList == null || nodeList.isEmpty()) {
                return null;
            }
            Map<String, Affiliation> affiliations = new HashMap<String, Affiliation>();
            for (Element node : nodeList) {
                affiliations.put(
                		node.getAttributeValue("id"), 
                		AffiliationBuilder.build(node));
            }
            return affiliations;
        } catch (JDOMException ex) {
            return null;
        }
	}
    
	private static List<Author> convertAuthors(Element source, List<String> affiliationsIds) {
        try {            
            XPath xPath = XPath.newInstance("/article/front//contrib-group/contrib[@contrib-type='author']");
            @SuppressWarnings("unchecked")
            List<Element> nodeList = xPath.selectNodes(source);
            if (nodeList == null || nodeList.isEmpty()) {
                return null;
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
            return null;
        }
	}
	
	/**
	 * Converts bibliographic references.
	 * @param id
	 * @param source
	 * @return list containing all bibliographic references
	 * @throws TransformationException 
	 */
	private static List<ReferenceMetadata> convertReferences(String id, Element source) throws TransformationException {
		Element backElement = source.getChild("back");
		if (backElement!=null) {
			Element refListElement = backElement.getChild("ref-list");
			if (refListElement!=null) {
				@SuppressWarnings("unchecked")
				List<Element> refs = refListElement.getChildren("ref");
				List<ReferenceMetadata> refMetas = new ArrayList<ReferenceMetadata>();
				if (refs!=null) {
					int idx = 1;
					NLMElementToBibEntryConverter bibEntryConverter = new NLMElementToBibEntryConverter();
					for (Element ref : refs) {
						if (ref.getChild("mixed-citation")!=null) {
							BibEntry bibEntry = bibEntryConverter.convert(
									ref.getChild("mixed-citation"));
							if (bibEntry!=null) {
								ReferenceMetadata.Builder refMetaBuilder = ReferenceMetadata.newBuilder();
								refMetaBuilder.setPosition(idx);
								refMetaBuilder.setText(bibEntry.getText());
								refMetaBuilder.setBasicMetadata(convertBibEntry(bibEntry));
								refMetas.add(refMetaBuilder.build());	
							} else {
								log.warn("got null bib-entry from element " + 
										ref.getChild("mixed-citation").getValue());
							}
							
						}
						idx++;
					}
				}
				return refMetas;
			}
		}
//		fallback
		return null;
	}
	
	/**
	 * Converts {@link BibEntry} to {@link ReferenceBasicMetadata}.
	 * @param entry
	 * @return {@link ReferenceBasicMetadata}
	 */
	public static ReferenceBasicMetadata convertBibEntry(BibEntry entry) {
		if (entry!=null) {
			ReferenceBasicMetadata.Builder builder = ReferenceBasicMetadata.newBuilder();
			List<String> resultValues = entry.getAllFieldValues(BibEntry.FIELD_AUTHOR);
			if (resultValues!=null && resultValues.size()>0) {
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
                    builder.setPages(
                            Range.newBuilder()
                            .setStart(m.group(1))
                            .setEnd(m.group(2))
                            .build());
                } else {
                    pagesPattern = Pattern.compile("^[0-9]+$");
                    m = pagesPattern.matcher(resultValue);
                    if (m.matches()) {
                        builder.setPages(
                                Range.newBuilder()
                                .setStart(m.group())
                                .setEnd(m.group())
                                .build());
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
	@SuppressWarnings("unchecked")
	private static ExtractedDocumentMetadata.Builder convertMeta(String id, Element source,
			ExtractedDocumentMetadata.Builder docMetaBuilder) {
		Element frontElement = source.getChild("front");
		if (frontElement!=null) {
			Element articleMetaElement = frontElement.getChild("article-meta");
			if (articleMetaElement!=null) {
//				/article/front/article-meta/title-group/article-title [multiple elements]		
				if (articleMetaElement.getChild("title-group")!=null) {
					List<Element> titles = articleMetaElement.getChild(
							"title-group").getChildren("article-title");
//					currenlty taking only first title into account!
					if (titles.size()>0) {
						if (titles.size()>1) {
							log.warn("got multiple titles for document " + id + 
									", storing first title only");
						}
						for (Element titleElem : titles) {
//							iterating until finding not null title
							String title = titleElem.getTextNormalize();
							if (title!=null && !title.isEmpty()) {
								docMetaBuilder.setTitle(title);
								break;
							}	
						}
					}
				}
//				/article/front/article-meta/abstract/p
				if (articleMetaElement.getChild("abstract")!=null &&
						articleMetaElement.getChild("abstract").getChild("p")!=null) {
					docMetaBuilder.setAbstract$(articleMetaElement.getChild(
							"abstract").getChild("p").getTextNormalize());
				}
//				/article/front/article-meta/kwd-group/kwd [multiple elements]
				if (articleMetaElement.getChild("kwd-group")!=null) {
					List<Element> keywords = articleMetaElement.getChild(
							"kwd-group").getChildren("kwd");
					for (Element keywordElem : keywords) {
						String keyword = keywordElem.getTextNormalize();
						if (keyword!=null && !keyword.isEmpty()) {
							if (docMetaBuilder.getKeywords()==null) {
								docMetaBuilder.setKeywords(new ArrayList<CharSequence>());
							}
							docMetaBuilder.getKeywords().add(keyword);
						}
					}
				}
//				/article/front/article-meta/article-id
				List<Element> articleIds = articleMetaElement.getChildren("article-id");
				Map<CharSequence, CharSequence> extIds = new HashMap<CharSequence, CharSequence>();
				for (Element articleIdElem : articleIds) {
					String idType = articleIdElem.getAttributeValue("pub-id-type");
					extIds.put(new Utf8(idType!=null?idType:HBaseConstants.EXTERNAL_ID_TYPE_UNKNOWN), 
							new Utf8(articleIdElem.getTextNormalize()));
				}
				if (!extIds.isEmpty()) {
					docMetaBuilder.setExternalIdentifiers(extIds);	
				}
//				/article/front/article-meta/pub-date/year
				if (articleMetaElement.getChild("pub-date")!=null &&
						articleMetaElement.getChild("pub-date").getChild("year")!=null) {
					try {
						docMetaBuilder.setYear(Integer.valueOf(articleMetaElement.getChild(
								"pub-date").getChild("year").getTextNormalize()));	
					} catch (Exception e) {
						log.error("unable to parse year, unsupported format: " + 
								articleMetaElement.getChild(
										"pub-date").getChild("year").getTextNormalize() + 
										", document id" + id, e);
					}
				}
			}
			Element journalMeta = frontElement.getChild("journal-meta");
			if (journalMeta!=null) {
//				/article/front/journal-meta/journal-title-group/journal-title
				if (journalMeta.getChild("journal-title-group")!=null) {
					List<Element> titles = journalMeta.getChild(
							"journal-title-group").getChildren("journal-title");
					if (titles.size()>0) {
						docMetaBuilder.setJournal(titles.iterator().next().getTextNormalize());
						if (titles.size()>1) {
							log.warn("got multiple journal titles, retrieving first title only. " +
									"Document id: " + id);
						}
					}
				}
//				/article/front/journal-meta/publisher/publisher-name
				if (journalMeta.getChild("publisher")!=null &&
						journalMeta.getChild("publisher").getChild("publisher-name")!=null) {
					String pubName = journalMeta.getChild("publisher").getChild("publisher-name").getTextNormalize();
					if (pubName!=null && !pubName.isEmpty()) {
						docMetaBuilder.setPublisher(pubName);
					}
				}
			}
		}
		return docMetaBuilder;
	}
	
	/**
	 * Converts given source element to {@link DocumentWithBasicMetadata}.
	 * Never returns null.
	 * @param id
	 * @param source
	 * @return {@link DocumentWithBasicMetadata}
	 * @throws JDOMException 
	 * @throws TransformationException 
	 */
	public static ExtractedDocumentMetadata convertFull(String id, Document source) throws JDOMException, TransformationException {
		if (id==null) {
			throw new RuntimeException("unable to set null id");
		}
		ExtractedDocumentMetadata.Builder builder = ExtractedDocumentMetadata.newBuilder();
		builder.setId(id);
		if (source==null) {
//			allowing returning empty extracted metadata
			return builder.build();
		}
        Element element = source.getRootElement();
		convertMeta(id, element, builder);
//		handling auxiliary metadata
		Element frontElement = element.getChild("front");
		if (frontElement!=null) {
			Element articleMetaElement = frontElement.getChild("article-meta");
			if (articleMetaElement!=null) {
//				/article/front/article-meta/volume
				if (articleMetaElement.getChild("volume")!=null) {
					builder.setVolume(articleMetaElement.getChild(
							"volume").getTextNormalize());
				}
//				/article/front/article-meta/issue
				if (articleMetaElement.getChild("issue")!=null) {
					builder.setIssue(articleMetaElement.getChild(
							"issue").getTextNormalize());
				}
//				/article/front/article-meta/fpage
//				/article/front/article-meta/lpage
				Range.Builder pagesBuilder = Range.newBuilder();
				if (articleMetaElement.getChild("fpage")!=null) {
					pagesBuilder.setStart(articleMetaElement.getChild(
							"fpage").getTextNormalize());
				}
				if (articleMetaElement.getChild("lpage")!=null) {
					pagesBuilder.setEnd(articleMetaElement.getChild(
							"lpage").getTextNormalize());
				}
				if (pagesBuilder.hasStart() || pagesBuilder.hasEnd()) {
					builder.setPages(pagesBuilder.build());
				}
			}
		}
        
        Map<String, Affiliation> affiliationMap = convertAffiliations(element);
        List<String> affiliationIds = new ArrayList<String>();
        List<Affiliation> affiliations = new ArrayList<Affiliation>();
    
        if (affiliationMap != null && !affiliationMap.isEmpty()) {
            for (Entry<String, Affiliation> entry : affiliationMap.entrySet()) {
                affiliationIds.add(entry.getKey());
                affiliations.add(entry.getValue());
            }
            builder.setAffiliations(affiliations);
        }
        
		List<Author> authors = convertAuthors(element, affiliationIds);
		if (authors != null && !authors.isEmpty()) {
			builder.setAuthors(authors);
		}
        
		List<ReferenceMetadata> refs = convertReferences(id, element);
		if (refs!=null && refs.size()>0) {
			builder.setReferences(refs);	
		}
		return builder.build();
	}
	
}
