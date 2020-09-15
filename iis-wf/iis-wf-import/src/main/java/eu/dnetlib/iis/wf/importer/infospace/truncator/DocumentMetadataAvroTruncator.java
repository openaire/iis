package eu.dnetlib.iis.wf.importer.infospace.truncator;

import eu.dnetlib.iis.importer.schemas.Author;
import eu.dnetlib.iis.importer.schemas.DocumentMetadata;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An implementation of {@link AvroTruncator} for {@link DocumentMetadata}.
 */
public class DocumentMetadataAvroTruncator implements AvroTruncator<DocumentMetadata> {
    public static final String PARAM_MAX_ABSTRACT_LENGTH = "maxAbstractLength";
    public static final String PARAM_MAX_TITLE_LENGTH = "maxTitleLength";
    public static final String PARAM_MAX_AUTHORS_SIZE = "maxAuthorsSize";
    public static final String PARAM_MAX_AUTHOR_FULLNAME_LENGTH = "maxAuthorFullnameLength";
    public static final String PARAM_MAX_KEYWORDS_SIZE = "maxKeywordsSize";
    public static final String PARAM_MAX_KEYWORD_LENGTH = "maxKeywordLength";

    private Function<CharSequence, CharSequence> abstractTruncator;
    private Function<CharSequence, CharSequence> titleTruncator;
    private Function<List<Author>, List<Author>> authorsTruncator;
    private Function<List<CharSequence>, List<CharSequence>> keywordsTruncator;

    private DocumentMetadataAvroTruncator(int maxAbstractLength,
                                          int maxTitleLength,
                                          int maxAuthorsSize,
                                          int maxAuthorFullnameLength,
                                          int maxKeywordsSize,
                                          int maxKeywordLength) {
        this.abstractTruncator = (Function<CharSequence, CharSequence> & Serializable) x ->
                StringTruncator.truncateWithoutWordSplit(x.toString(), maxAbstractLength);

        this.titleTruncator = (Function<CharSequence, CharSequence> & Serializable) x ->
                StringTruncator.truncateWithoutWordSplit(x.toString(), maxTitleLength);

        AuthorAvroTruncator authorAvroTruncator = AuthorAvroTruncator.newBuilder()
                .setMaxAuthorFullnameLength(maxAuthorFullnameLength)
                .build();
        this.authorsTruncator = (Function<List<Author>, List<Author>> & Serializable) xs ->
                xs.stream()
                        .limit(maxAuthorsSize)
                        .map(authorAvroTruncator::truncate)
                        .collect(Collectors.toList());

        Function<CharSequence, CharSequence> keywordTruncator = (Function<CharSequence, CharSequence> & Serializable) x ->
                StringTruncator.truncateWithoutWordSplit(x.toString(), maxKeywordLength);
        this.keywordsTruncator = (Function<List<CharSequence>, List<CharSequence>> & Serializable) xs ->
                xs.stream()
                        .limit(maxKeywordsSize)
                        .map(keywordTruncator)
                        .collect(Collectors.toList());
    }

    @Override
    public DocumentMetadata truncate(DocumentMetadata source) {
        if (Objects.nonNull(source.getAbstract$())) {
            source.setAbstract$(abstractTruncator.apply(source.getAbstract$()));
        }

        if (Objects.nonNull(source.getTitle())) {
            source.setTitle(titleTruncator.apply(source.getTitle()));
        }

        if (Objects.nonNull(source.getAuthors())) {
            source.setAuthors(authorsTruncator.apply(source.getAuthors()));
        }

        if (Objects.nonNull(source.getKeywords())) {
            source.setKeywords(keywordsTruncator.apply(source.getKeywords()));
        }

        return source;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private int maxAbstractLength;
        private int maxTitleLength;
        private int maxAuthorsSize;
        private int maxAuthorFullnameLength;
        private int maxKeywordsSize;
        private int maxKeywordLength;

        public Builder setMaxAbstractLength(int maxAbstractLength) {
            this.maxAbstractLength = maxAbstractLength;
            return this;
        }

        public Builder setMaxTitleLength(int maxTitleLength) {
            this.maxTitleLength = maxTitleLength;
            return this;
        }

        public Builder setMaxAuthorsSize(int maxAuthorsSize) {
            this.maxAuthorsSize = maxAuthorsSize;
            return this;
        }

        public Builder setMaxAuthorFullnameLength(int maxAuthorFullnameLength) {
            this.maxAuthorFullnameLength = maxAuthorFullnameLength;
            return this;
        }

        public Builder setMaxKeywordsSize(int maxKeywordsSize) {
            this.maxKeywordsSize = maxKeywordsSize;
            return this;
        }

        public Builder setMaxKeywordLength(int maxKeywordLength) {
            this.maxKeywordLength = maxKeywordLength;
            return this;
        }

        public DocumentMetadataAvroTruncator build() {
            return new DocumentMetadataAvroTruncator(
                    maxAbstractLength,
                    maxTitleLength,
                    maxAuthorsSize,
                    maxAuthorFullnameLength,
                    maxKeywordsSize,
                    maxKeywordLength
            );
        }
    }
}
