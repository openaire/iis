package eu.dnetlib.iis.workflows.sentimentanalysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import eu.dnetlib.iis.metadataextraction.schemas.ReferenceBasicMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata;
import eu.dnetlib.iis.sentimentanalysis.schemas.DocumentTextWithReferences;
import eu.dnetlib.iis.sentimentanalysis.schemas.ReferenceWithSentimentLabels;
import pl.edu.icm.cermine.ComponentFactory;
import pl.edu.icm.cermine.ContentExtractor;
import pl.edu.icm.cermine.bibref.model.BibEntry;
import pl.edu.icm.cermine.bibref.sentiment.model.CiTOProperty;
import pl.edu.icm.cermine.bibref.sentiment.model.CitationSentiment;
import pl.edu.icm.cermine.exception.AnalysisException;


/**
 * Sentiment analysis module. Facade to cermine's sentiment extraction.
 * @author mhorst
 */
public class SentimentAnalyzer extends Mapper<AvroKey<DocumentTextWithReferences>, NullWritable, AvroKey<ReferenceWithSentimentLabels>, NullWritable> {

	private final Logger log = Logger.getLogger(this.getClass());
	
	@Override
    protected void map(AvroKey<DocumentTextWithReferences> key, NullWritable value, Context context)
            throws IOException, InterruptedException {
		DocumentTextWithReferences input = key.datum();
		try {
			ContentExtractor extractor = new ContentExtractor();
//			currently using random implementation
			extractor.getConf().setCitationSentimentAnalyser(
					ComponentFactory.getRandomCitationSentimentAnalyser());
			extractor.setRawFullText(input.getText().toString());
			extractor.setReferences(convertReferences(input.getReferences()));
			List<ReferenceWithSentimentLabels> results = convertCitationSentiment(
					input.getId(), input.getReferences(), extractor.getCitationSentiments());
			if (results!=null && results.size()>0) {
				for (ReferenceWithSentimentLabels result : results) {
					context.write(new AvroKey<ReferenceWithSentimentLabels>(result), 
	                		NullWritable.get());	
				}
			}
		} catch (AnalysisException e) {
			log.error("exception occurred when extracting sentiment for document: " + 
					input.getId(), e);
		}
    }
	
	/**
	 * Converts citation sentiments into {@link ReferenceWithSentimentLabels} objects.
	 * @param publicationId
	 * @param references
	 * @param sentiments
	 * @return citation sentiments converted into {@link ReferenceWithSentimentLabels} objects
	 */
	protected static List<ReferenceWithSentimentLabels> convertCitationSentiment(
			CharSequence publicationId,
			List<ReferenceMetadata> references,
			List<CitationSentiment> sentiments) {
		if (references.size()!=sentiments.size()) {
			throw new RuntimeException("number of sentiments: " + sentiments.size() + 
					" is not equal the number of input references: " + references.size());
		}
		List<ReferenceWithSentimentLabels> results = new ArrayList<ReferenceWithSentimentLabels>(
				references.size());
		for (int i=0; i<references.size(); i++) {
			ReferenceWithSentimentLabels result = convertCitationSentiment(
					publicationId, references.get(i), sentiments.get(i));
			if (result!=null) {
				results.add(result);
			}
		}
		return results;
	}
	
	/**
	 * Converts citation sentiment into {@link ReferenceWithSentimentLabels}.
	 * May return null when no sentiment was generated.
	 * @param publicationId
	 * @param reference
	 * @param sentiment
	 * @return citation sentiment converted to {@link ReferenceWithSentimentLabels} or null when no sentiment generated
	 */
	protected static ReferenceWithSentimentLabels convertCitationSentiment(
			CharSequence publicationId,
			ReferenceMetadata reference, CitationSentiment sentiment) {
		if (sentiment!=null && !sentiment.getProperties().isEmpty()) {
			ReferenceWithSentimentLabels.Builder builder = ReferenceWithSentimentLabels.newBuilder();
			builder.setId(publicationId);
			builder.setReferencePosition(reference.getPosition());
			builder.setLabels(generateLabels(sentiment));
			return builder.build();
		} else {
			return null;
		}
	}
	
	/**
	 * Generates list of sentiment labels as URIs from CitationSentiment object.
	 * @param sentiment
	 * @return list of sentiment labels as URIs
	 */
	protected static List<CharSequence> generateLabels(CitationSentiment sentiment) {
		Set<CiTOProperty> props = sentiment.getProperties();
		List<CharSequence> results = new ArrayList<CharSequence>(props.size());
		for (CiTOProperty prop : props) {
			results.add(prop.getUri());
		}
		return results;
	}
	
	/**
	 * Converts references to {@link BibEntry} objects.
	 * @param references
	 * @return references converted to {@link BibEntry} objects
	 */
	protected static List<BibEntry> convertReferences(List<ReferenceMetadata> references) {
		BibEntry[] results = new BibEntry[references.size()];
		int idx = 0;
		for (ReferenceMetadata currentReference : references) {
			BibEntry bibEntry = new BibEntry();
			if (currentReference.getText()!=null) {
				bibEntry.setText(currentReference.getText().toString());
//				setting metadata fields
				ReferenceBasicMetadata basicMetadata = currentReference.getBasicMetadata();
				if (basicMetadata.getAuthors()!=null) {
					for (CharSequence author : basicMetadata.getAuthors()) {
						bibEntry.addField(BibEntry.FIELD_AUTHOR, author.toString());
					}
				}
				if (basicMetadata.getPages()!=null) {
					if (basicMetadata.getPages().getStart()!=null && 
							basicMetadata.getPages().getStart().length()>0) {
						if (basicMetadata.getPages().getEnd()!=null && 
								basicMetadata.getPages().getEnd().length()>0) {
//							range
							StringBuilder pagesField = new StringBuilder(
									basicMetadata.getPages().getStart());
							pagesField.append('-');
							pagesField.append(basicMetadata.getPages().getEnd());
							bibEntry.addField(BibEntry.FIELD_PAGES, 
									pagesField.toString());
						} else {
//							start only
							bibEntry.addField(BibEntry.FIELD_PAGES, 
									basicMetadata.getPages().getStart().toString());
						}
					}
				}
				if (basicMetadata.getSource()!=null) {
					bibEntry.addField(BibEntry.FIELD_JOURNAL,
							basicMetadata.getSource().toString());
				} 
				if (basicMetadata.getTitle()!=null) {
					bibEntry.addField(BibEntry.FIELD_TITLE,
							basicMetadata.getTitle().toString());
				}
				if (basicMetadata.getVolume()!=null) {
					bibEntry.addField(BibEntry.FIELD_VOLUME,
							basicMetadata.getVolume().toString());
				}
				if (basicMetadata.getYear()!=null) {
					bibEntry.addField(BibEntry.FIELD_YEAR,
							basicMetadata.getYear().toString());
				}
				if (basicMetadata.getEdition()!=null) {
					bibEntry.addField(BibEntry.FIELD_EDITION,
							basicMetadata.getEdition().toString());
				}
				if (basicMetadata.getPublisher()!=null) {
					bibEntry.addField(BibEntry.FIELD_PUBLISHER,
							basicMetadata.getPublisher().toString());
				}
				if (basicMetadata.getLocation()!=null) {
					bibEntry.addField(BibEntry.FIELD_LOCATION,
							basicMetadata.getLocation().toString());
				}
				if (basicMetadata.getSeries()!=null) {
					bibEntry.addField(BibEntry.FIELD_SERIES,
							basicMetadata.getSeries().toString());
				}
				if (basicMetadata.getIssue()!=null) {
					bibEntry.addField(BibEntry.FIELD_NUMBER,
							basicMetadata.getIssue().toString());
				}
				if (basicMetadata.getUrl()!=null) {
					bibEntry.addField(BibEntry.FIELD_URL,
							basicMetadata.getUrl().toString());
				}
			}
			results[idx] = bibEntry;
			idx++;
		}
		return Arrays.asList(results);
	}
}
