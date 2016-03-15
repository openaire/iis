package eu.dnetlib.iis.wf.collapsers.origins;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import eu.dnetlib.iis.common.citations.schemas.Citation;
import eu.dnetlib.iis.common.citations.schemas.CitationEntry;
import eu.dnetlib.iis.wf.collapsers.origins.PMCCitationCollapser;

/**
 * {@link PMCCitationCollapser} test class.
 * @author mhorst
 *
 */
public class PMCCitationCollapserTest {

	@Test
	public void testCollapsingWhenPmcTargetIdSet() throws Exception {
		PMCCitationCollapser collapser = new PMCCitationCollapser();
		Map<String,List<Citation>> objects = new HashMap<String, List<Citation>>();
		
		String sourceId = "sourceId";
		String pmcTargetId = "pmcTargetId";
		String cermineTargetId = "cermineTargetId";
		String text = "citation text";
		
		objects.put("ingested", Arrays.asList(new Citation[] {
				Citation
						.newBuilder()
						.setSourceDocumentId(sourceId)
						.setEntry(
								CitationEntry.newBuilder()
										.setPosition(1)
										.setConfidenceLevel(1f)
										.setDestinationDocumentId(pmcTargetId)
										.setExternalDestinationDocumentIds(new HashMap<CharSequence, CharSequence>())
										.setRawText(text).build()).build()		
		}));
		objects.put("matched", Arrays.asList(new Citation[] {
				Citation
						.newBuilder()
						.setSourceDocumentId(sourceId)
						.setEntry(
								CitationEntry
										.newBuilder()
										.setPosition(1)
										.setConfidenceLevel(0.1f)
										.setDestinationDocumentId(cermineTargetId)
										.setExternalDestinationDocumentIds(new HashMap<CharSequence, CharSequence>())
										.setRawText(text).build()).build()
		}));
		
		List<Citation> results = collapser.collapseBetweenOrigins(objects);
		assertNotNull(results);
		assertEquals(1, results.size());
		assertEquals(pmcTargetId,results.get(0).getEntry().getDestinationDocumentId());
		assertEquals(new Float(1f),results.get(0).getEntry().getConfidenceLevel());
	}
	
	@Test
	public void testCollapsingWhenPmcTargetIdNotSet() throws Exception {
		PMCCitationCollapser collapser = new PMCCitationCollapser();
		Map<String,List<Citation>> objects = new HashMap<String, List<Citation>>();
		
		String sourceId = "sourceId";
		String cermineTargetId = "cermineTargetId";
		String text = "citation text";
		
		objects.put("ingested", Arrays.asList(new Citation[] {
				Citation
						.newBuilder()
						.setSourceDocumentId(sourceId)
						.setEntry(
								CitationEntry.newBuilder()
										.setPosition(1)
										.setExternalDestinationDocumentIds(new HashMap<CharSequence, CharSequence>())
										.setRawText(text).build()).build()		
		}));
		objects.put("matched", Arrays.asList(new Citation[] {
				Citation
						.newBuilder()
						.setSourceDocumentId(sourceId)
						.setEntry(
								CitationEntry
										.newBuilder()
										.setPosition(1)
										.setConfidenceLevel(0.1f)
										.setDestinationDocumentId(cermineTargetId)
										.setExternalDestinationDocumentIds(new HashMap<CharSequence, CharSequence>())
										.setRawText(text).build()).build()
		}));
		
		List<Citation> results = collapser.collapseBetweenOrigins(objects);
		assertNotNull(results);
		assertEquals(1, results.size());
		assertEquals(cermineTargetId,results.get(0).getEntry().getDestinationDocumentId());
		assertEquals(new Float(0.1f),results.get(0).getEntry().getConfidenceLevel());
	}
	
	@Test
	public void testCollapsingWithDifferentText() throws Exception {
		PMCCitationCollapser collapser = new PMCCitationCollapser();
		Map<String,List<Citation>> objects = new HashMap<String, List<Citation>>();
		
		String sourceId = "sourceId";
		String pmcTargetId = "pmcTargetId";
		String cermineTargetId = "cermineTargetId";
		String textPmc = "pmc citation text";
		String textCermine = "cermine citation text";
		
		objects.put("ingested", Arrays.asList(new Citation[] {
				Citation
						.newBuilder()
						.setSourceDocumentId(sourceId)
						.setEntry(
								CitationEntry.newBuilder()
										.setPosition(1)
										.setConfidenceLevel(1f)
										.setDestinationDocumentId(pmcTargetId)
										.setExternalDestinationDocumentIds(new HashMap<CharSequence, CharSequence>())
										.setRawText(textPmc).build()).build()		
		}));
		objects.put("matched", Arrays.asList(new Citation[] {
				Citation
						.newBuilder()
						.setSourceDocumentId(sourceId)
						.setEntry(
								CitationEntry
										.newBuilder()
										.setPosition(1)
										.setConfidenceLevel(0.1f)
										.setDestinationDocumentId(cermineTargetId)
										.setExternalDestinationDocumentIds(new HashMap<CharSequence, CharSequence>())
										.setRawText(textCermine).build()).build()
		}));
		
		List<Citation> results = collapser.collapseBetweenOrigins(objects);
		assertNotNull(results);
		assertEquals(2, results.size());
		assertEquals(pmcTargetId,results.get(0).getEntry().getDestinationDocumentId());
		assertEquals(new Float(1f),results.get(0).getEntry().getConfidenceLevel());
		assertEquals(textPmc,results.get(0).getEntry().getRawText());
		
		assertEquals(cermineTargetId,results.get(1).getEntry().getDestinationDocumentId());
		assertEquals(new Float(0.1f),results.get(1).getEntry().getConfidenceLevel());
		assertEquals(textCermine,results.get(1).getEntry().getRawText());
	}
}
