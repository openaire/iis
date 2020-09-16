package eu.dnetlib.iis.wf.importer.infospace.truncator;

import eu.dnetlib.iis.importer.schemas.DataSetReference;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An implementation of {@link AvroTruncator} for {@link DataSetReference}.
 */
public class DataSetReferenceAvroTruncator implements AvroTruncator<DataSetReference> {
    private Function<List<CharSequence>, List<CharSequence>> creatorNamesTruncator;
    private Function<List<CharSequence>, List<CharSequence>> titlesTruncator;
    private Function<CharSequence, CharSequence> descriptionTruncator;

    private DataSetReferenceAvroTruncator(int maxCreatorNamesSize,
                                          int maxCreatorNameLength,
                                          int maxTitlesSize,
                                          int maxTitleLength,
                                          int maxDescriptionLength) {
        Function<CharSequence, CharSequence> creatorNameTruncator = (Function<CharSequence, CharSequence> & Serializable) x ->
                StringTruncator.truncateWithoutWordSplit(x.toString(), maxCreatorNameLength);
        this.creatorNamesTruncator = (Function<List<CharSequence>, List<CharSequence>> & Serializable) xs ->
                xs.stream()
                        .limit(maxCreatorNamesSize)
                        .map(creatorNameTruncator)
                        .collect(Collectors.toList());

        Function<CharSequence, CharSequence> titleTruncator = (Function<CharSequence, CharSequence> & Serializable) x ->
                StringTruncator.truncateWithoutWordSplit(x.toString(), maxTitleLength);
        this.titlesTruncator = (Function<List<CharSequence>, List<CharSequence>> & Serializable) xs ->
                xs.stream()
                        .limit(maxTitlesSize)
                        .map(titleTruncator)
                        .collect(Collectors.toList());

        this.descriptionTruncator = (Function<CharSequence, CharSequence> & Serializable) x ->
                StringTruncator.truncateWithoutWordSplit(x.toString(), maxDescriptionLength);
    }

    @Override
    public DataSetReference truncate(DataSetReference source) {
        if (Objects.nonNull(source.getCreatorNames())) {
            source.setCreatorNames(creatorNamesTruncator.apply(source.getCreatorNames()));
        }

        if (Objects.nonNull(source.getTitles())) {
            source.setTitles(titlesTruncator.apply(source.getTitles()));
        }

        if (Objects.nonNull(source.getDescription())) {
            source.setDescription(descriptionTruncator.apply(source.getDescription()));
        }

        return source;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private int maxCreatorNamesSize;
        private int maxCreatorNameLength;
        private int maxTitlesSize;
        private int maxTitleLength;
        private int maxDescriptionLength;

        public Builder setMaxCreatorNamesSize(int maxCreatorNamesSize) {
            this.maxCreatorNamesSize = maxCreatorNamesSize;
            return this;
        }

        public Builder setMaxCreatorNameLength(int maxCreatorNameLength) {
            this.maxCreatorNameLength = maxCreatorNameLength;
            return this;
        }

        public Builder setMaxTitlesSize(int maxTitlesSize) {
            this.maxTitlesSize = maxTitlesSize;
            return this;
        }

        public Builder setMaxTitleLength(int maxTitleLength) {
            this.maxTitleLength = maxTitleLength;
            return this;
        }

        public Builder setMaxDescriptionLength(int maxDescriptionLength) {
            this.maxDescriptionLength = maxDescriptionLength;
            return this;
        }

        public DataSetReferenceAvroTruncator build() {
            return new DataSetReferenceAvroTruncator(maxCreatorNamesSize,
                    maxCreatorNameLength,
                    maxTitlesSize,
                    maxTitleLength,
                    maxDescriptionLength);
        }
    }
}
