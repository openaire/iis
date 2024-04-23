package eu.dnetlib.iis.wf.export.actionmanager;


import org.apache.commons.lang3.EnumUtils;

public enum PidType {

    /**
     * The DOI syntax shall be made up of a DOI prefix and a DOI suffix separated by a forward slash.
     *
     * There is no defined limit on the length of the DOI name, or of the DOI prefix or DOI suffix.
     *
     * The DOI name is case-insensitive and can incorporate any printable characters from the legal graphic characters
     * of Unicode. Further constraints on character use (e.g. use of language-specific alphanumeric characters) can be
     * defined for an application by the ISO 26324 Registration Authority.
     *
     *
     * DOI prefix: The DOI prefix shall be composed of a directory indicator followed by a registrant code.
     * These two components shall be separated by a full stop (period). The directory indicator shall be "10" and
     * distinguishes the entire set of character strings (prefix and suffix) as digital object identifiers within the
     * resolution system.
     *
     * Registrant code: The second element of the DOI prefix shall be the registrant code. The registrant code is a
     * unique string assigned to a registrant.
     *
     * DOI suffix: The DOI suffix shall consist of a character string of any length chosen by the registrant.
     * Each suffix shall be unique to the prefix element that precedes it. The unique suffix can be a sequential number,
     * or it might incorporate an identifier generated from or based on another system used by the registrant
     * (e.g. ISAN, ISBN, ISRC, ISSN, ISTC, ISNI; in such cases, a preferred construction for such a suffix can be
     * specified, as in Example 1).
     *
     * Source: https://www.doi.org/doi_handbook/2_Numbering.html#2.2
     */
    doi,

    /**
     * PubMed Unique Identifier (PMID)
     *
     * This field is a 1-to-8 digit accession number with no leading zeros. It is present on all records and is the
     * accession number for managing and disseminating records. PMIDs are not reused after records are deleted.
     *
     * Beginning in February 2012 PMIDs include extensions following a decimal point to account for article versions
     * (e.g., 21804956.2). All citations are considered version 1 until replaced.  The extended PMID is not displayed
     * on the MEDLINE format.
     *
     * View the citation in abstract format in PubMed to access additional versions when available (see the article in
     * the Jan-Feb 2012 NLM Technical Bulletin).
     *
     * Source: https://www.nlm.nih.gov/bsd/mms/medlineelements.html#pmid
     */
    pmid,

    /**
     * This field contains the unique identifier for the cited article in PubMed Central. The identifier begins with the
     * prefix PMC.
     *
     * Source: https://www.nlm.nih.gov/bsd/mms/medlineelements.html#pmc
     */
    pmc, handle, arXiv, nct, pdb, w3id,

    // Organization
    openorgs, ROR, GRID, PIC, ISNI, Wikidata, FundRef, corda, corda_h2020, mag_id, urn,

    // Used by dedup
    undefined, original;

    public static boolean isValid(String type) {
        return EnumUtils.isValidEnum(PidType.class, type);
    }

    public static PidType tryValueOf(String s) {
        try {
            return PidType.valueOf(s);
        } catch (Exception e) {
            return PidType.original;
        }
    }

}