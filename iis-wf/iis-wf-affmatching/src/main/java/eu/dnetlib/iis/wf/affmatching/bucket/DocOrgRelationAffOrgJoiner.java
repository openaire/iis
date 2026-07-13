package eu.dnetlib.iis.wf.affmatching.bucket;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentOrganization;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.DocumentOrganizationFetcher;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import scala.Tuple2;

/**
 * Implementation of {@link AffOrgJoiner} that joins {@link AffMatchAffiliation} with {@link AffMatchOrganization}
 * based on document-organization relations.
 * 
 * @author madryk
 */
public class DocOrgRelationAffOrgJoiner implements AffOrgJoiner {

    private static final long serialVersionUID = 1L;

    /**
     * Maximum number of organization associations allowed per document when building the join key.
     * Documents linked to more organizations than this threshold (e.g. a paper with many grant partners)
     * are the primary cause of data skew: one task handles the entire Cartesian product of 
     * (affiliations × organizations) for a hot document ID, producing partitions orders of
     * magnitude larger than the median. Such documents contribute very little signal for affiliation
     * matching because the affiliation–organization pairing becomes essentially random at this scale.
     * Tune this value based on observed skew; 100 is a conservative starting point.
     */
    static final int MAX_ORGANIZATIONS_PER_DOCUMENT = 100;

    private DocumentOrganizationFetcher documentOrganizationFetcher;


    //------------------------ LOGIC --------------------------

    /**
     * Joins the given affiliations with organizations based on document-organization relations.<br />
     * Method uses {@link DocumentOrganizationFetcher} internally to fetch document-organization pairs.<br/>
     * 
     * Affiliation will be joined with the organization if fetched document-organization relations will
     * contain pair ({@link AffMatchAffiliation#getDocumentId()}, {@link AffMatchOrganization#getId()}).<br/>
     *
     * Documents whose document-organization fanout exceeds {@link #MAX_ORGANIZATIONS_PER_DOCUMENT}
     * are excluded from the join to prevent data skew (see constant javadoc for rationale).
     */
    @Override
    public JavaRDD<Tuple2<AffMatchAffiliation, AffMatchOrganization>> join(JavaRDD<AffMatchAffiliation> affiliations, JavaRDD<AffMatchOrganization> organizations) {
        
        JavaPairRDD<String, AffMatchAffiliation> affiliationsDocIdKey = affiliations.keyBy(aff -> aff.getDocumentId());
        JavaPairRDD<String, AffMatchOrganization> organizationsOrgIdKey = organizations.keyBy(org -> org.getId());
        
        JavaRDD<AffMatchDocumentOrganization> documentOrganizations = documentOrganizationFetcher.fetchDocumentOrganizations();

        // Cache to avoid recomputing the document-organization pipeline twice
        documentOrganizations.cache();

        // Count organizations per document to identify skewed (high-fanout) keys.
        // Documents linked to an excessive number of organizations via project chains create
        // Cartesian-product partitions in the join below that dwarf all other partitions combined.
        JavaPairRDD<String, Long> orgCountPerDoc = documentOrganizations
                .mapToPair(docOrg -> new Tuple2<>(docOrg.getDocumentId(), 1L))
                .reduceByKey(Long::sum);

        // Keep only document-organization pairs whose document has a manageable fanout.
        JavaPairRDD<String, AffMatchDocumentOrganization> documentOrganizationDocIdKey = documentOrganizations
                .keyBy(docOrg -> docOrg.getDocumentId())
                .join(orgCountPerDoc)
                .filter(x -> x._2._2 <= MAX_ORGANIZATIONS_PER_DOCUMENT)
                .mapValues(v -> v._1);

        JavaRDD<Tuple2<AffMatchAffiliation, AffMatchOrganization>> affOrgBucketPairs = affiliationsDocIdKey
                .join(documentOrganizationDocIdKey)
                .mapToPair(x -> new Tuple2<String, AffMatchAffiliation>(x._2._2.getOrganizationId(), x._2._1))
                .join(organizationsOrgIdKey)
                .values();

        documentOrganizations.unpersist();

        return affOrgBucketPairs;
    }


    //------------------------ SETTERS --------------------------
    
    public void setDocumentOrganizationFetcher(DocumentOrganizationFetcher documentOrganizationFetcher) {
        this.documentOrganizationFetcher = documentOrganizationFetcher;
    }

}
