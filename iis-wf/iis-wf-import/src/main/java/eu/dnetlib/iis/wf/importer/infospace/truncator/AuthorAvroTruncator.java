package eu.dnetlib.iis.wf.importer.infospace.truncator;

import eu.dnetlib.iis.importer.schemas.Author;

import java.io.Serializable;
import java.util.Objects;
import java.util.function.Function;

/**
 * An implementation of {@link AvroTruncator} for {@link Author}.
 */
public class AuthorAvroTruncator implements AvroTruncator<Author> {
    private Function<CharSequence, CharSequence> fullnameTruncator;

    private AuthorAvroTruncator(int maxAuthorFullnameLength) {
        this.fullnameTruncator = (Function<CharSequence, CharSequence> & Serializable) x ->
                StringTruncator.truncateWithoutWordSplit(x.toString(), maxAuthorFullnameLength);
    }

    @Override
    public Author truncate(Author source) {
        if (Objects.nonNull(source.getFullname())) {
            source.setFullname(fullnameTruncator.apply(source.getFullname()));
        }

        return source;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private int maxAuthorFullnameLength;

        public Builder setMaxAuthorFullnameLength(int maxAuthorFullnameLength) {
            this.maxAuthorFullnameLength = maxAuthorFullnameLength;
            return this;
        }

        public AuthorAvroTruncator build() {
            return new AuthorAvroTruncator(maxAuthorFullnameLength);
        }
    }
}
