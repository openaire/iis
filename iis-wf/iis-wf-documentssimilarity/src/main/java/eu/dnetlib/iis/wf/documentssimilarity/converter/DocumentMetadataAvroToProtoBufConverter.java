package eu.dnetlib.iis.wf.documentssimilarity.converter;

import com.google.common.base.Objects;
import eu.dnetlib.iis.common.protobuf.AvroToProtoBufConverter;
import eu.dnetlib.iis.documentssimilarity.schemas.DocumentMetadata;
import eu.dnetlib.iis.importer.schemas.Person;
import org.apache.commons.lang3.StringUtils;
import pl.edu.icm.coansys.models.DocumentProtos;

import java.util.Collections;
import java.util.LinkedList;

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
public class DocumentMetadataAvroToProtoBufConverter implements AvroToProtoBufConverter<DocumentMetadata, DocumentProtos.DocumentWrapper> {
    @Override
    public String convertIntoKey(DocumentMetadata datum) {
        return datum.getId().toString();
    }

    @Override
    public DocumentProtos.DocumentWrapper convertIntoValue(DocumentMetadata datum) {
        DocumentProtos.DocumentWrapper.Builder docBuilder = DocumentProtos.DocumentWrapper.newBuilder();
        DocumentProtos.DocumentMetadata.Builder metaBuilder = DocumentProtos.DocumentMetadata.newBuilder();
        DocumentProtos.BasicMetadata.Builder basicMetaBuilder = DocumentProtos.BasicMetadata.newBuilder();
        DocumentProtos.KeywordsList.Builder keywordsBuilder = DocumentProtos.KeywordsList.newBuilder();

        docBuilder.setRowId(datum.getId().toString());
        metaBuilder.setKey(datum.getId().toString());

        if (datum.getAbstract$() != null) {
            metaBuilder.addDocumentAbstract(createTextWithNoLanguage(datum.getAbstract$().toString()));
        }

        if (datum.getTitle() != null) {
            basicMetaBuilder.addTitle(createTextWithNoLanguage(datum.getTitle().toString()));
        }

        for (Person author : Objects.firstNonNull(datum.getAuthors(), Collections.<Person>emptyList())) {
            basicMetaBuilder.addAuthor(convertAuthor(author));
        }

        for (CharSequence keyword : Objects.firstNonNull(datum.getKeywords(), Collections.<CharSequence>emptyList())) {
            keywordsBuilder.addKeywords(keyword.toString());
        }

        metaBuilder.setBasicMetadata(basicMetaBuilder);
        metaBuilder.addKeywords(keywordsBuilder);
        docBuilder.setDocumentMetadata(metaBuilder);
        return docBuilder.build();
    }

    private static DocumentProtos.TextWithLanguage.Builder createTextWithNoLanguage(String s) {
        return DocumentProtos.TextWithLanguage.newBuilder().setText(s);
    }

    private static DocumentProtos.Author.Builder convertAuthor(Person author) {
        DocumentProtos.Author.Builder authorBuilder = DocumentProtos.Author.newBuilder();
        if (author.getId() != null) {
            authorBuilder.setKey(author.getId().toString());
        } else {
            authorBuilder.setKey("FAKE_KEY");
        }

        LinkedList<String> nameTokens = new LinkedList<String>();

        if (author.getFirstname() != null) {
            nameTokens.add(author.getFirstname().toString());
        }
        for(CharSequence token : Objects.firstNonNull(author.getSecondnames(), Collections.<CharSequence>emptyList())) {
            nameTokens.add(token.toString());
        }
        if (!nameTokens.isEmpty()) {
            authorBuilder.setSurname(nameTokens.getLast());
            nameTokens.removeLast();
        }
        if (!nameTokens.isEmpty()) {
            authorBuilder.setForenames(StringUtils.join(nameTokens, ' '));
        }

        if (author.getFullname() != null) {
            authorBuilder.setName(author.getFullname().toString());
        }

        return  authorBuilder;
    }
}
