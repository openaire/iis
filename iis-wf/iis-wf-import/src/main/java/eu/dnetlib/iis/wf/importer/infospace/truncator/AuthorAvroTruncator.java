package eu.dnetlib.iis.wf.importer.infospace.truncator;

import eu.dnetlib.iis.importer.schemas.Author;
import eu.dnetlib.iis.wf.importer.infospace.ImportInformationSpaceConstants;

import java.io.Serializable;
import java.util.Objects;
import java.util.function.Function;

/**
 * An implementation of {@link AvroTruncator} for {@link Author}.
 */
public class AuthorAvroTruncator implements AvroTruncator<Author> {
    private FullnameTruncator fullnameTruncator =
            new FullnameTruncator(ImportInformationSpaceConstants.MAX_AUTHOR_FULLNAME_LENGTH);

    @Override
    public Author truncate(Author source) {
        if (Objects.nonNull(source.getFullname())) {
            source.setFullname(fullnameTruncator.truncate(source.getFullname()));
        }

        return source;
    }

    public static class FullnameTruncator implements Serializable {
        private Function<CharSequence, CharSequence> stringTruncator;

        public FullnameTruncator(int maxAuthorFullnameLength) {
            this.stringTruncator = (Function<CharSequence, CharSequence> & Serializable) x ->
                    StringTruncator.truncateWithoutWordSplit(x.toString(), maxAuthorFullnameLength);
        }

        public void setStringTruncator(Function<CharSequence, CharSequence> stringTruncator) {
            this.stringTruncator = stringTruncator;
        }

        CharSequence truncate(CharSequence x) {
            return stringTruncator.apply(x);
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private Integer maxAuthorFullnameLength;

        public Builder setMaxAuthorFullnameLength(Integer maxAuthorFullnameLength) {
            this.maxAuthorFullnameLength = maxAuthorFullnameLength;
            return this;
        }

        public AuthorAvroTruncator build() {
            AuthorAvroTruncator instance = new AuthorAvroTruncator();

            if (Objects.nonNull(maxAuthorFullnameLength)) {
                instance.fullnameTruncator = new FullnameTruncator(maxAuthorFullnameLength);
            }

            return instance;
        }
    }
}
