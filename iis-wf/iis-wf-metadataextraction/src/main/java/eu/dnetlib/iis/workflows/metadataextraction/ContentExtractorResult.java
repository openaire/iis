package eu.dnetlib.iis.workflows.metadataextraction;

import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;

public class ContentExtractorResult {
	protected Exception e;
	protected ExtractedDocumentMetadata meta;
	protected DocumentText text;
	public ContentExtractorResult(Exception e) {
		this.e = e;
	}
	public ContentExtractorResult(ExtractedDocumentMetadata meta,
			DocumentText text) {
		this.meta = meta;
		this.text = text;
	}
}