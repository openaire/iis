package eu.dnetlib.iis.wf.importer.infospace.truncator;

import eu.dnetlib.iis.importer.schemas.Author;
import eu.dnetlib.iis.importer.schemas.DocumentMetadata;
import eu.dnetlib.iis.wf.importer.infospace.ImportInformationSpaceConstants;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An implementation of {@link AvroTruncator} for {@link DocumentMetadata}.
 */
public class DocumentMetadataAvroTruncator implements AvroTruncator<DocumentMetadata> {
    private AbstractTruncator abstractTruncator =
            new AbstractTruncator(ImportInformationSpaceConstants.MAX_ABSTRACT_LENGTH);
    private TitleTruncator titleTruncator =
            new TitleTruncator(ImportInformationSpaceConstants.MAX_TITLE_LENGTH);
    private AuthorsTruncator authorsTruncator =
            new AuthorsTruncator(ImportInformationSpaceConstants.MAX_AUTHORS_SIZE, ImportInformationSpaceConstants.MAX_AUTHOR_FULLNAME_LENGTH);
    private KeywordsTruncator keywordsTruncator =
            new KeywordsTruncator(ImportInformationSpaceConstants.MAX_KEYWORDS_SIZE, ImportInformationSpaceConstants.MAX_KEYWORD_LENGTH);

    @Override
    public DocumentMetadata truncate(DocumentMetadata source) {
        if (Objects.nonNull(source.getAbstract$())) {
            source.setAbstract$(abstractTruncator.truncate(source.getAbstract$()));
        }

        if (Objects.nonNull(source.getTitle())) {
            source.setTitle(titleTruncator.truncate(source.getTitle()));
        }

        if (Objects.nonNull(source.getAuthors())) {
            source.setAuthors(authorsTruncator.truncate(source.getAuthors()));
        }

        if (Objects.nonNull(source.getKeywords())) {
            source.setKeywords(keywordsTruncator.truncate(source.getKeywords()));
        }

        return source;
    }

    public static class AbstractTruncator implements Serializable {
        private Function<CharSequence, CharSequence> stringTruncator;

        public AbstractTruncator(int maxAbstractLength) {
            this.stringTruncator = (Function<CharSequence, CharSequence> & Serializable) x ->
                    StringTruncator.truncateWithoutWordSplit(x.toString(), maxAbstractLength);
        }

        public void setStringTruncator(Function<CharSequence, CharSequence> stringTruncator) {
            this.stringTruncator = stringTruncator;
        }

        CharSequence truncate(CharSequence x) {
            return stringTruncator.apply(x);
        }
    }

    public static class TitleTruncator implements Serializable {
        private Function<CharSequence, CharSequence> stringTruncator;

        public TitleTruncator(int maxTitleLength) {
            this.stringTruncator = (Function<CharSequence, CharSequence> & Serializable) x ->
                    StringTruncator.truncateWithoutWordSplit(x.toString(), maxTitleLength);
        }

        public void setStringTruncator(Function<CharSequence, CharSequence> stringTruncator) {
            this.stringTruncator = stringTruncator;
        }

        CharSequence truncate(CharSequence x) {
            return stringTruncator.apply(x);
        }
    }

    public static class AuthorsTruncator implements Serializable {
        private Function<List<Author>, List<Author>> listTruncator;
        private AuthorAvroTruncator authorAvroTruncator;

        public AuthorsTruncator(int maxAuthorsSize, int maxAuthorFullnameLength) {
            this.listTruncator = (Function<List<Author>, List<Author>> & Serializable) xs ->
                    xs.stream().limit(maxAuthorsSize).collect(Collectors.toList());
            this.authorAvroTruncator = AuthorAvroTruncator.newBuilder()
                    .setMaxAuthorFullnameLength(maxAuthorFullnameLength)
                    .build();
        }

        public void setListTruncator(Function<List<Author>, List<Author>> listTruncator) {
            this.listTruncator = listTruncator;
        }

        public void setAuthorAvroTruncator(AuthorAvroTruncator authorAvroTruncator) {
            this.authorAvroTruncator = authorAvroTruncator;
        }

        List<Author> truncate(List<Author> xs) {
            return listTruncator.apply(xs)
                    .stream()
                    .map(x -> authorAvroTruncator.truncate(x))
                    .collect(Collectors.toList());
        }
    }

    public static class KeywordsTruncator implements Serializable {
        private Function<List<CharSequence>, List<CharSequence>> listTruncator;
        private Function<CharSequence, CharSequence> stringTruncator;

        public KeywordsTruncator(int maxKeywordsSize, int maxKeywordLength) {
            this.listTruncator = (Function<List<CharSequence>, List<CharSequence>> & Serializable) xs ->
                    xs.stream().limit(maxKeywordsSize).collect(Collectors.toList());
            this.stringTruncator = (Function<CharSequence, CharSequence> & Serializable) x ->
                    StringTruncator.truncateWithoutWordSplit(x.toString(), maxKeywordLength);
        }

        public void setListTruncator(Function<List<CharSequence>, List<CharSequence>> listTruncator) {
            this.listTruncator = listTruncator;
        }

        public void setStringTruncator(Function<CharSequence, CharSequence> stringTruncator) {
            this.stringTruncator = stringTruncator;
        }

        List<CharSequence> truncate(List<CharSequence> xs) {
            return listTruncator.apply(xs)
                    .stream()
                    .map(x -> stringTruncator.apply(x))
                    .collect(Collectors.toList());
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private Integer maxAbstractLength;
        private Integer maxTitleLength;
        private Integer maxAuthorsSize;
        private Integer maxAuthorFullnameLength;
        private Integer maxKeywordsSize;
        private Integer maxKeywordLength;

        public Builder setMaxAbstractLength(int maxAbstractLength) {
            this.maxAbstractLength = maxAbstractLength;
            return this;
        }

        public Builder setMaxTitleLength(Integer maxTitleLength) {
            this.maxTitleLength = maxTitleLength;
            return this;
        }

        public Builder setMaxAuthorsSize(Integer maxAuthorsSize) {
            this.maxAuthorsSize = maxAuthorsSize;
            return this;
        }

        public Builder setMaxAuthorFullnameLength(Integer maxAuthorFullnameLength) {
            this.maxAuthorFullnameLength = maxAuthorFullnameLength;
            return this;
        }

        public Builder setMaxKeywordsSize(Integer maxKeywordsSize) {
            this.maxKeywordsSize = maxKeywordsSize;
            return this;
        }

        public Builder setMaxKeywordLength(Integer maxKeywordLength) {
            this.maxKeywordLength = maxKeywordLength;
            return this;
        }

        public DocumentMetadataAvroTruncator build() {
            DocumentMetadataAvroTruncator instance = new DocumentMetadataAvroTruncator();

            if (Objects.nonNull(maxAbstractLength)) {
                instance.abstractTruncator = new AbstractTruncator(maxAbstractLength);
            }

            if (Objects.nonNull(maxTitleLength)) {
                instance.titleTruncator = new TitleTruncator(maxTitleLength);
            }

            if (Objects.nonNull(maxAuthorsSize) && Objects.nonNull(maxAuthorFullnameLength)) {
                instance.authorsTruncator = new AuthorsTruncator(maxAuthorsSize, maxAuthorFullnameLength);
            }

            if (Objects.nonNull(maxKeywordsSize) && Objects.nonNull(maxKeywordLength)) {
                instance.keywordsTruncator = new KeywordsTruncator(maxKeywordsSize, maxKeywordLength);
            }

            return instance;
        }
    }
}
