package eu.dnetlib.iis.wf.importer.infospace.converter;

import com.google.common.base.Preconditions;

import eu.dnetlib.data.proto.OafProtos.OafRel;
import eu.dnetlib.iis.importer.schemas.DocumentToProject;

/**
 * Result-project {@link OafRel} relation to {@link DocumentToProject} converter.
 * 
 * @author mhorst
 *
 */
public class DocumentToProjectRelationConverter implements OafRelToAvroConverter<DocumentToProject> {

    @Override
    public DocumentToProject convert(OafRel oafRel) {
        Preconditions.checkNotNull(oafRel);
        DocumentToProject.Builder builder = DocumentToProject.newBuilder();
        builder.setDocumentId(oafRel.getSource());
        builder.setProjectId(oafRel.getTarget());
        return builder.build();
    }
}