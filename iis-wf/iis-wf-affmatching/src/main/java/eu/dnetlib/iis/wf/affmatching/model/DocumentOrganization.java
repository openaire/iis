package eu.dnetlib.iis.wf.affmatching.model;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Paired document and organization identifiers.
 * 
 * @author mhorst
 *
 */
public class DocumentOrganization {

    private String documentId;

    private String organizationId;

    // ------------------------ CONSTRUCTORS --------------------------

    public DocumentOrganization(String documentId, String organizationId) {
        Preconditions.checkArgument(StringUtils.isNotBlank(documentId));
        Preconditions.checkArgument(StringUtils.isNotBlank(organizationId));
        this.documentId = documentId;
        this.organizationId = organizationId;
    }

    // ------------------------ GETTERS --------------------------

    /**
     * Document identifier.
     */
    public String getDocumentId() {
        return documentId;
    }

    /**
     * Organization identifier.
     */
    public String getOrganizationId() {
        return organizationId;
    }

    // ------------------------ SETTERS --------------------------

    public void setDocumentId(String documentId) {
        this.documentId = documentId;
    }

    public void setOrganizationId(String organizationId) {
        this.organizationId = organizationId;
    }

    // ------------------------ HashCode & Equals ----------------

    @Override
    public int hashCode() {
        return Objects.hashCode(documentId, organizationId);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DocumentOrganization) {
            final DocumentOrganization other = (DocumentOrganization) obj;
            return Objects.equal(documentId, other.getDocumentId())
                    && Objects.equal(organizationId, other.getOrganizationId());
        } else {
            return false;
        }
    }
}
