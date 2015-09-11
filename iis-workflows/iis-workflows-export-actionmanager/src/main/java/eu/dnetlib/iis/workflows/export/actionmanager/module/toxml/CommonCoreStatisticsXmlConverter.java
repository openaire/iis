package eu.dnetlib.iis.workflows.export.actionmanager.module.toxml;

import java.util.HashMap;
import java.util.Map;

import eu.dnetlib.iis.common.model.extrainfo.converter.AbstractExtraInfoConverter;
import eu.dnetlib.iis.statistics.schemas.BasicCitationStatistics;
import eu.dnetlib.iis.statistics.schemas.CommonCoreStatistics;
import eu.dnetlib.iis.statistics.schemas.CoreStatistics;
import eu.dnetlib.iis.statistics.schemas.ExtendedStatistics;

/**
 * {@link CommonCoreStatistics} based avro to xml converter.
 * @author mhorst
 *
 */
public class CommonCoreStatisticsXmlConverter extends AbstractExtraInfoConverter<CommonCoreStatistics> {

	public CommonCoreStatisticsXmlConverter() {
		xstream.alias("statistics", CommonCoreStatistics.class);
	}
	
	public static void main(String[] args) {
		
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
		publishedPapersMap.put("10", 20);
		publishedPapersMap.put("100", 210);
		
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

		CommonCoreStatisticsXmlConverter converter = new CommonCoreStatisticsXmlConverter();
		System.out.println(converter.serialize(coreStatsBuilder.build()));
	}

	
}
