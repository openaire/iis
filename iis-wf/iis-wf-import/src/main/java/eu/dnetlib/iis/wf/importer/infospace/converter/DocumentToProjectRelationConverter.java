package eu.dnetlib.iis.wf.importer.infospace.converter;

import com.google.common.base.Preconditions;

import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.importer.schemas.DocumentToProject;

/**
 * Result-project {@link Relation} relation to {@link DocumentToProject} converter.
 * 
 * @author mhorst
 *
 */
public class DocumentToProjectRelationConverter implements OafRelToAvroConverter<DocumentToProject> {

    @Override
    public DocumentToProject convert(Relation relation) {
        Preconditions.checkNotNull(relation);
        DocumentToProject.Builder builder = DocumentToProject.newBuilder();
        builder.setDocumentId(relation.getSource());
        builder.setProjectId(relation.getTarget());
        return builder.build();
    }
}