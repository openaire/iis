package eu.dnetlib.iis.citationmatching.converter;

import eu.dnetlib.iis.citationmatching.schemas.BasicMetadata;
import pl.edu.icm.coansys.models.DocumentProtos;

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
public final class Util {
    private Util() {}

    private final static String AUTHOR_KEY = "EMPTY_AUTHOR_KEY";

    static DocumentProtos.BasicMetadata avroBasicMetadataToProtoBuf(BasicMetadata avroBasicMeta) {
        DocumentProtos.BasicMetadata.Builder metaBuilder = DocumentProtos.BasicMetadata.newBuilder();

        for (CharSequence authorName : avroBasicMeta.getAuthors()) {
            DocumentProtos.Author.Builder authorBuilder = DocumentProtos.Author.newBuilder();
            authorBuilder.setKey(AUTHOR_KEY);
            authorBuilder.setName(authorName.toString());
            metaBuilder.addAuthor(authorBuilder);
        }

        if (avroBasicMeta.getTitle() != null)
            metaBuilder.addTitle(DocumentProtos.TextWithLanguage.newBuilder().setText(avroBasicMeta.getTitle().toString()));
        if (avroBasicMeta.getPages() != null)
            metaBuilder.setPages(avroBasicMeta.getPages().toString());
        if (avroBasicMeta.getYear() != null)
            metaBuilder.setYear(avroBasicMeta.getYear().toString());
        if (avroBasicMeta.getJournal() != null)
            metaBuilder.setJournal(avroBasicMeta.getJournal().toString());

        return metaBuilder.build();
    }
}
