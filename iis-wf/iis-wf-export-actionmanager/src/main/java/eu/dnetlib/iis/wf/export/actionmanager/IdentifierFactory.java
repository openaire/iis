package eu.dnetlib.iis.wf.export.actionmanager;

import static com.google.common.base.Preconditions.checkArgument;
import static eu.dnetlib.dhp.schema.common.ModelConstants.ARXIV_ID;
import static eu.dnetlib.dhp.schema.common.ModelConstants.CROSSREF_ID;
import static eu.dnetlib.dhp.schema.common.ModelConstants.DATACITE_ID;
import static eu.dnetlib.dhp.schema.common.ModelConstants.EUROPE_PUBMED_CENTRAL_ID;
import static eu.dnetlib.dhp.schema.common.ModelConstants.OPEN_APC_ID;
import static eu.dnetlib.dhp.schema.common.ModelConstants.OPEN_APC_NAME;
import static eu.dnetlib.dhp.schema.common.ModelConstants.PUBMED_CENTRAL_ID;
import static eu.dnetlib.dhp.schema.common.ModelConstants.ROHUB_ID;
import static eu.dnetlib.dhp.schema.common.ModelConstants.ZENODO_OD_ID;
import static eu.dnetlib.dhp.schema.common.ModelConstants.ZENODO_R3_ID;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;

import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.Instance;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

/**
 * Factory class for OpenAIRE identifiers in the Graph
 */
public class IdentifierFactory implements Serializable {

    public static final String ID_SEPARATOR = "::";
    public static final String ID_PREFIX_SEPARATOR = "|";

    public static final int ID_PREFIX_LEN = 12;

    /**
     * Declares the associations PID_TYPE -> [DATASOURCE ID, NAME] considered authoritative for that PID_TYPE.
     * The id of the record (source_::id) will be rewritten as pidType_::id)
     */
    public static final Map<PidType, HashBiMap<String, String>> PID_AUTHORITY = Maps.newHashMap();

    static {
        PID_AUTHORITY.put(PidType.doi, HashBiMap.create());
        PID_AUTHORITY.get(PidType.doi).put(CROSSREF_ID, "Crossref");
        PID_AUTHORITY.get(PidType.doi).put(DATACITE_ID, "Datacite");
        PID_AUTHORITY.get(PidType.doi).put(ZENODO_OD_ID, "ZENODO");
        PID_AUTHORITY.get(PidType.doi).put(ZENODO_R3_ID, "Zenodo");

        PID_AUTHORITY.put(PidType.pmc, HashBiMap.create());
        PID_AUTHORITY.get(PidType.pmc).put(EUROPE_PUBMED_CENTRAL_ID, "Europe PubMed Central");
        PID_AUTHORITY.get(PidType.pmc).put(PUBMED_CENTRAL_ID, "PubMed Central");

        PID_AUTHORITY.put(PidType.pmid, HashBiMap.create());
        PID_AUTHORITY.get(PidType.pmid).put(EUROPE_PUBMED_CENTRAL_ID, "Europe PubMed Central");
        PID_AUTHORITY.get(PidType.pmid).put(PUBMED_CENTRAL_ID, "PubMed Central");

        PID_AUTHORITY.put(PidType.arXiv, HashBiMap.create());
        PID_AUTHORITY.get(PidType.arXiv).put(ARXIV_ID, "arXiv.org e-Print Archive");

        PID_AUTHORITY.put(PidType.w3id, HashBiMap.create());
        PID_AUTHORITY.get(PidType.w3id).put(ROHUB_ID, "ROHub");
    }

    /**
     * Declares the associations PID_TYPE -> [DATASOURCE ID, PID SUBSTRING] considered as delegated authority for that
     * PID_TYPE. Example, Zenodo is delegated to forge DOIs that contain the 'zenodo' word.
     *
     * If a record with the same id (same pid) comes from 2 data sources, the one coming from a delegated source wins. E.g. Zenodo records win over those from Datacite.
     * See also https://code-repo.d4science.org/D-Net/dnet-hadoop/pulls/187 and the class dhp-common/src/main/java/eu/dnetlib/dhp/schema/oaf/utils/OafMapperUtils.java
     */
    public static final Map<PidType, Map<String, String>> DELEGATED_PID_AUTHORITY = Maps.newHashMap();

    static {
        DELEGATED_PID_AUTHORITY.put(PidType.doi, new HashMap<>());
        DELEGATED_PID_AUTHORITY.get(PidType.doi).put(ZENODO_OD_ID, "zenodo");
        DELEGATED_PID_AUTHORITY.get(PidType.doi).put(ZENODO_R3_ID, "zenodo");
        DELEGATED_PID_AUTHORITY.put(PidType.w3id, new HashMap<>());
        DELEGATED_PID_AUTHORITY.get(PidType.w3id).put(ROHUB_ID, "ro-id");
    }

    /**
     * Declares the associations PID_TYPE -> [DATASOURCE ID, NAME] whose records are considered enrichment for the graph.
     * Their OpenAIRE ID is built from the declared PID type. Are merged with their corresponding record, identified by
     * the same OpenAIRE id.
     */
    public static final Map<PidType, HashBiMap<String, String>> ENRICHMENT_PROVIDER = Maps.newHashMap();

    static {
        ENRICHMENT_PROVIDER.put(PidType.doi, HashBiMap.create());
        ENRICHMENT_PROVIDER.get(PidType.doi).put(OPEN_APC_ID, OPEN_APC_NAME);
    }

    /**
     * Creates an identifier from the most relevant PID (if available) provided by a known PID authority in the given
     * entity T. Returns entity.id when none of the PIDs meet the selection criteria is available.
     *
     * @param entity the entity providing PIDs and a default ID.
     * @param <T> the specific entity type. Currently Organization and Result subclasses are supported.
     * @param md5 indicates whether should hash the PID value or not.
     * @return an identifier from the most relevant PID, entity.id otherwise
     */
    public static <T extends OafEntity> String createIdentifier(T entity, boolean md5) {

        checkArgument(StringUtils.isNoneBlank(entity.getId()), "missing entity identifier");

        final Map<String, Set<StructuredProperty>> pids = extractPids(entity);

        return pids
            .values()
            .stream()
            .flatMap(Set::stream)
            .min(new PidComparator<>(entity))
            .map(
                min -> Optional
                    .ofNullable(pids.get(min.getQualifier().getClassid()))
                    .map(
                        p -> p
                            .stream()
                            .sorted(new PidValueComparator())
                            .findFirst()
                            .map(s -> idFromPid(entity, s, md5))
                            .orElseGet(entity::getId))
                    .orElseGet(entity::getId))
            .orElseGet(entity::getId);
    }

    private static <T extends OafEntity> Map<String, Set<StructuredProperty>> extractPids(T entity) {
        if (entity instanceof Result) {
            return Optional
                .ofNullable(((Result) entity).getInstance())
                .map(IdentifierFactory::mapPids)
                .orElse(new HashMap<>());
        } else {
            return entity
                .getPid()
                .stream()
                .map(CleaningFunctions::normalizePidValue)
                .filter(CleaningFunctions::pidFilter)
                .collect(
                    Collectors
                        .groupingBy(
                            p -> p.getQualifier().getClassid(),
                            Collectors.mapping(p -> p, Collectors.toCollection(HashSet::new))));
        }
    }

    private static Map<String, Set<StructuredProperty>> mapPids(List<Instance> instance) {
        return instance
            .stream()
            .map(i -> pidFromInstance(i.getPid(), i.getCollectedfrom(), false))
            .flatMap(Function.identity())
            .collect(
                Collectors
                    .groupingBy(
                        p -> p.getQualifier().getClassid(),
                        Collectors.mapping(p -> p, Collectors.toCollection(HashSet::new))));
    }

    private static Stream<StructuredProperty> pidFromInstance(List<StructuredProperty> pid, KeyValue collectedFrom,
        boolean mapHandles) {
        return Optional
            .ofNullable(pid)
            .map(
                pp -> pp
                    .stream()
                    // filter away PIDs provided by a DS that is not considered an authority for the
                    // given PID Type
                    .filter(p -> shouldFilterPidByCriteria(collectedFrom, p, mapHandles))
                    .map(CleaningFunctions::normalizePidValue)
                    .filter(p -> isNotFromDelegatedAuthority(collectedFrom, p))
                    .filter(CleaningFunctions::pidFilter))
            .orElse(Stream.empty());
    }

    private static boolean shouldFilterPidByCriteria(KeyValue collectedFrom, StructuredProperty p, boolean mapHandles) {
        final PidType pType = PidType.tryValueOf(p.getQualifier().getClassid());

        if (Objects.isNull(collectedFrom)) {
            return false;
        }

        boolean isEnrich = Optional
            .ofNullable(ENRICHMENT_PROVIDER.get(pType))
            .map(
                enrich -> enrich.containsKey(collectedFrom.getKey())
                    || enrich.containsValue(collectedFrom.getValue()))
            .orElse(false);

        boolean isAuthority = Optional
            .ofNullable(PID_AUTHORITY.get(pType))
            .map(
                authorities -> authorities.containsKey(collectedFrom.getKey())
                    || authorities.containsValue(collectedFrom.getValue()))
            .orElse(false);

        return (mapHandles && pType.equals(PidType.handle)) || isEnrich || isAuthority;
    }

    private static boolean isNotFromDelegatedAuthority(KeyValue collectedFrom, StructuredProperty p) {
        final PidType pType = PidType.tryValueOf(p.getQualifier().getClassid());

        final Map<String, String> da = DELEGATED_PID_AUTHORITY.get(pType);
        if (Objects.isNull(da)) {
            return true;
        }
        if (!da.containsKey(collectedFrom.getKey())) {
            return true;
        }
        return StringUtils.contains(p.getValue(), da.get(collectedFrom.getKey()));
    }

    /**
     * @see {@link IdentifierFactory#createIdentifier(OafEntity, boolean)}
     */
    public static <T extends OafEntity> String createIdentifier(T entity) {

        return createIdentifier(entity, true);
    }

    private static <T extends OafEntity> String idFromPid(T entity, StructuredProperty s, boolean md5) {
        return idFromPid(ModelSupport.getIdPrefix(entity.getClass()), s.getQualifier().getClassid(), s.getValue(), md5);
    }

    public static String idFromPid(String numericPrefix, String pidType, String pidValue, boolean md5) {
        return new StringBuilder()
            .append(numericPrefix)
            .append(ID_PREFIX_SEPARATOR)
            .append(createPrefix(pidType))
            .append(ID_SEPARATOR)
            .append(md5 ? md5(pidValue) : pidValue)
            .toString();
    }

    // create the prefix (length = 12)
    private static String createPrefix(String pidType) {
        StringBuilder prefix = new StringBuilder(StringUtils.left(pidType, ID_PREFIX_LEN));
        while (prefix.length() < ID_PREFIX_LEN) {
            prefix.append("_");
        }
        return prefix.substring(0, ID_PREFIX_LEN);
    }

    public static String md5(final String s) {
        try {
            final MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(s.getBytes(StandardCharsets.UTF_8));
            return new String(Hex.encodeHex(md.digest()));
        } catch (final Exception e) {
            return null;
        }
    }

}