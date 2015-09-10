package eu.dnetlib.iis.export.actionmanager.module.toxml;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.dnetlib.iis.common.model.extrainfo.converter.AbstractExtraInfoConverter;
import eu.dnetlib.iis.statistics.schemas.AuthorStatistics;
import eu.dnetlib.iis.statistics.schemas.BasicCitationStatistics;
import eu.dnetlib.iis.statistics.schemas.CoAuthor;
import eu.dnetlib.iis.statistics.schemas.CommonCoreStatistics;
import eu.dnetlib.iis.statistics.schemas.CoreStatistics;
import eu.dnetlib.iis.statistics.schemas.ExtendedStatistics;

/**
 * {@link AuthorStatistics} based avro to xml converter.
 * @author mhorst
 *
 */
public class AuthorStatisticsXmlConverter extends AbstractExtraInfoConverter<AuthorStatistics> {

	public AuthorStatisticsXmlConverter() {
		xstream.alias("statistics", AuthorStatistics.class);
		xstream.alias("coAuthor", CoAuthor.class);
		xstream.addImplicitCollection(AuthorStatistics.class, "coAuthors");
	}
	
	public static void main(String[] args) {
		AuthorStatistics.Builder authorStatsBuilder = AuthorStatistics.newBuilder();
		
		List<CoAuthor> coAuthors = new ArrayList<CoAuthor>();
		CoAuthor.Builder coauthorBuilder = CoAuthor.newBuilder();
		coauthorBuilder.setCoauthoredPapersCount(1);
		coauthorBuilder.setId("someAuthorId");
		coAuthors.add(coauthorBuilder.build());
		coauthorBuilder.setCoauthoredPapersCount(2);
		coauthorBuilder.setId("someOtherAuthorId");
		coAuthors.add(coauthorBuilder.build());
		authorStatsBuilder.setCoAuthors(coAuthors);
		
		CommonCoreStatistics.Builder coreStatsBuilder = CommonCoreStatistics.newBuilder();
		
//		all
		{
		CoreStatistics.Builder allPapersStatsBuilder = CoreStatistics.newBuilder();
		allPapersStatsBuilder.setNumberOfPapers(200);
		ExtendedStatistics.Builder fromAllPapersBuilder = ExtendedStatistics.newBuilder(); 
		fromAllPapersBuilder.setAverageNumberOfCitationsPerPaper(3.5f);
		BasicCitationStatistics.Builder allPapersBasicStatsBuilder = BasicCitationStatistics.newBuilder();
		allPapersBasicStatsBuilder.setNumberOfCitations(500);
		Map<CharSequence, Integer> allCitationsPerYear = new HashMap<CharSequence, Integer>();
		allCitationsPerYear.put("1999", 13);
		allCitationsPerYear.put("1980", 2);
		allPapersBasicStatsBuilder.setNumberOfCitationsPerYear(allCitationsPerYear);
		fromAllPapersBuilder.setBasic(allPapersBasicStatsBuilder.build());
		Map<CharSequence, Integer> allPapersMap = new HashMap<CharSequence, Integer>();
		allPapersMap.put("10", 3);
		fromAllPapersBuilder.setNumberOfPapersCitedAtLeastXTimes(allPapersMap);
		allPapersStatsBuilder.setCitationsFromAllPapers(fromAllPapersBuilder.build());
		
		ExtendedStatistics.Builder fromPublishedPapersBuilder = ExtendedStatistics.newBuilder(); 
		fromPublishedPapersBuilder.setAverageNumberOfCitationsPerPaper(2.5f);
		BasicCitationStatistics.Builder publishedPapersBasicStatsBuilder = BasicCitationStatistics.newBuilder();
		publishedPapersBasicStatsBuilder.setNumberOfCitations(200);
		Map<CharSequence, Integer> publishedCitationsPerYear = new HashMap<CharSequence, Integer>();
		publishedCitationsPerYear.put("1980", 13);
		publishedPapersBasicStatsBuilder.setNumberOfCitationsPerYear(allCitationsPerYear);
		fromPublishedPapersBuilder.setBasic(allPapersBasicStatsBuilder.build());
		Map<CharSequence, Integer> publishedPapersMap = new HashMap<CharSequence, Integer>();
//		TODO set papers
		fromPublishedPapersBuilder.setNumberOfPapersCitedAtLeastXTimes(publishedPapersMap);
		allPapersStatsBuilder.setCitationsFromPublishedPapers(fromPublishedPapersBuilder.build());
		
		coreStatsBuilder.setAllPapers(allPapersStatsBuilder.build());
		}
		
//		published
		{
		CoreStatistics.Builder publishedPapersStatsBuilder = CoreStatistics.newBuilder();
		publishedPapersStatsBuilder.setNumberOfPapers(100);
		
		ExtendedStatistics.Builder fromAllPapersBuilder = ExtendedStatistics.newBuilder(); 
		fromAllPapersBuilder.setAverageNumberOfCitationsPerPaper(3.5f);
		BasicCitationStatistics.Builder allPapersBasicStatsBuilder = BasicCitationStatistics.newBuilder();
		allPapersBasicStatsBuilder.setNumberOfCitations(500);
		Map<CharSequence, Integer> allCitationsPerYear = new HashMap<CharSequence, Integer>();
		allCitationsPerYear.put("2009", 4);
		allPapersBasicStatsBuilder.setNumberOfCitationsPerYear(allCitationsPerYear);
		fromAllPapersBuilder.setBasic(allPapersBasicStatsBuilder.build());
		Map<CharSequence, Integer> allPapersMap = new HashMap<CharSequence, Integer>();
//		TODO set papers
		fromAllPapersBuilder.setNumberOfPapersCitedAtLeastXTimes(allPapersMap);
		publishedPapersStatsBuilder.setCitationsFromAllPapers(fromAllPapersBuilder.build());
		
		ExtendedStatistics.Builder fromPublishedPapersBuilder = ExtendedStatistics.newBuilder(); 
		fromPublishedPapersBuilder.setAverageNumberOfCitationsPerPaper(2.5f);
		BasicCitationStatistics.Builder publishedPapersBasicStatsBuilder = BasicCitationStatistics.newBuilder();
		publishedPapersBasicStatsBuilder.setNumberOfCitations(200);
		Map<CharSequence, Integer> publishedCitationsPerYear = new HashMap<CharSequence, Integer>();
		publishedCitationsPerYear.put("2001", 2);
		publishedPapersBasicStatsBuilder.setNumberOfCitationsPerYear(allCitationsPerYear);
		fromPublishedPapersBuilder.setBasic(allPapersBasicStatsBuilder.build());
		Map<CharSequence, Integer> publishedPapersMap = new HashMap<CharSequence, Integer>();
//		TODO set papers
		fromPublishedPapersBuilder.setNumberOfPapersCitedAtLeastXTimes(publishedPapersMap);
		publishedPapersStatsBuilder.setCitationsFromPublishedPapers(fromPublishedPapersBuilder.build());

		coreStatsBuilder.setPublishedPapers(publishedPapersStatsBuilder.build());
		}
		
		
		authorStatsBuilder.setCore(coreStatsBuilder.build());
		
		AuthorStatisticsXmlConverter converter = new AuthorStatisticsXmlConverter();
		System.out.println(converter.serialize(authorStatsBuilder.build()));
	}

	
}
