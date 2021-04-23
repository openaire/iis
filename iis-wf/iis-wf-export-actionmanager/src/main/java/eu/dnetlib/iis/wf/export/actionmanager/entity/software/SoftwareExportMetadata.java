package eu.dnetlib.iis.wf.export.actionmanager.entity.software;

import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.DocumentToSoftwareUrlWithMeta;

import java.util.Set;

public class SoftwareExportMetadata {
    private final DocumentToSoftwareUrlWithMeta documentToSoftwareUrlWithMeta;
    private final Set<CharSequence> title;
    private final String documentId;
    private final String softwareId;
    private final String softwareUrl;

    public SoftwareExportMetadata(DocumentToSoftwareUrlWithMeta documentToSoftwareUrlWithMeta,
                                  Set<CharSequence> title,
                                  String documentId,
                                  String softwareId,
                                  String softwareUrl) {
        this.documentToSoftwareUrlWithMeta = documentToSoftwareUrlWithMeta;
        this.title = title;
        this.documentId = documentId;
        this.softwareId = softwareId;
        this.softwareUrl = softwareUrl;
    }

    public DocumentToSoftwareUrlWithMeta getDocumentToSoftwareUrlWithMeta() {
        return documentToSoftwareUrlWithMeta;
    }

    public Set<CharSequence> getTitle() {
        return title;
    }

    public String getDocumentId() {
        return documentId;
    }

    public String getSoftwareId() {
        return softwareId;
    }

    public String getSoftwareUrl() {
        return softwareUrl;
    }
}
