package eu.dnetlib.iis.workflows.export.actionmanager.module.toxml;

import java.util.HashMap;
import java.util.Map;

import eu.dnetlib.iis.common.model.extrainfo.converter.AbstractExtraInfoConverter;
import eu.dnetlib.iis.statistics.schemas.BasicCitationStatistics;
import eu.dnetlib.iis.statistics.schemas.CommonBasicCitationStatistics;

/**
 * {@link CommonBasicCitationStatistics} based avro to xml converter.
 * @author mhorst
 *
 */
public class CommonBasicCitationStatisticsXmlConverter extends AbstractExtraInfoConverter<CommonBasicCitationStatistics> {

	public CommonBasicCitationStatisticsXmlConverter() {
		xstream.alias("statistics", CommonBasicCitationStatistics.class);
	}
	
	public static void main(String[] args) {
		CommonBasicCitationStatistics.Builder commonStatsBuilder = CommonBasicCitationStatistics.newBuilder();
		
		BasicCitationStatistics.Builder allPapersStatsBuilder = BasicCitationStatistics.newBuilder();
		allPapersStatsBuilder.setNumberOfCitations(500);
		Map<CharSequence, Integer> allCitationsPerYear = new HashMap<CharSequence, Integer>();
		allCitationsPerYear.put("1999", 13);
		allPapersStatsBuilder.setNumberOfCitationsPerYear(allCitationsPerYear);
		commonStatsBuilder.setCitationsFromAllPapers(allPapersStatsBuilder.build());
		
		BasicCitationStatistics.Builder publishedPapersStatsBuilder = BasicCitationStatistics.newBuilder();
		publishedPapersStatsBuilder.setNumberOfCitations(500);
		Map<CharSequence, Integer> publishedCitationsPerYear = new HashMap<CharSequence, Integer>();
		publishedCitationsPerYear.put("2013", 2);
		publishedPapersStatsBuilder.setNumberOfCitationsPerYear(publishedCitationsPerYear);
		commonStatsBuilder.setCitationsFromPublishedPapers(publishedPapersStatsBuilder.build());
		
		CommonBasicCitationStatisticsXmlConverter converter = new CommonBasicCitationStatisticsXmlConverter();
		
		System.out.println(converter.serialize(commonStatsBuilder.build()));
	}

	
}
