package eu.dnetlib.iis.wf.export.actionmanager.entity.patent;

import eu.dnetlib.iis.referenceextraction.patent.schemas.DocumentToPatent;
import eu.dnetlib.iis.referenceextraction.patent.schemas.Patent;

public class PatentExportMetadata {
    private final DocumentToPatent documentToPatent;
    private final Patent patent;
    private final String documentId;
    private final String patentId;

    public PatentExportMetadata(DocumentToPatent documentToPatent, Patent patent, String documentId, String patentId) {
        this.documentToPatent = documentToPatent;
        this.patent = patent;
        this.documentId = documentId;
        this.patentId = patentId;
    }

    public DocumentToPatent getDocumentToPatent() {
        return documentToPatent;
    }

    public Patent getPatent() {
        return patent;
    }

    public String getDocumentId() {
        return documentId;
    }

    public String getPatentId() {
        return patentId;
    }
}
