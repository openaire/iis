package eu.dnetlib.iis.wf.export.actionmanager.entity.patent;

import eu.dnetlib.iis.referenceextraction.patent.schemas.DocumentToPatent;

public class DocumentToPatentWithIdsToExport {
    private DocumentToPatent documentToPatent;
    private String documentIdToExport;
    private String patentIdToExport;

    public DocumentToPatentWithIdsToExport(DocumentToPatent documentToPatent, String documentIdToExport, String patentIdToExport) {
        this.documentToPatent = documentToPatent;
        this.documentIdToExport = documentIdToExport;
        this.patentIdToExport = patentIdToExport;
    }

    public DocumentToPatent getDocumentToPatent() {
        return documentToPatent;
    }

    public String getDocumentIdToExport() {
        return documentIdToExport;
    }

    public String getPatentIdToExport() {
        return patentIdToExport;
    }
}
