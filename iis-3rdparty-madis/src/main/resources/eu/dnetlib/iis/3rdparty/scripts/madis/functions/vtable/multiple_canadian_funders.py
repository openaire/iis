# coding=utf-8  # We use non-ASCII, french chars inside the regexes.
import vtbase
from functions.row.text import regexprmatches

registered = True

class CanadianVT(vtbase.VT):

    cihr_regex = unicode(".*(?:(?:CIHR|IRSC)|(?i)(?:canad(?:ian|a) institute(?:s)? health research|institut(?:(?:e)?(?:s)?)? recherche sant(?:é|e) canada)).*", "utf_8")
    nserc_regex = unicode(".*(?:(?:NSERC|CRSNG)|(?i)(?:nat(?:ural|ional) science(?:s)?(?:\sengineering(?:\sresearch)?|\sresearch) co(?:u)?n(?:c|se)(?:i)?l|conseil(?:s)? recherche(?:s)? science(?:s)? naturel(?:les)?(?:\sg(?:e|é)nie)? canada)).*", "utf_8")
    sshrc_regex = unicode(".*(?:(?:SSHRC|CRSH|SSRCC)|(?i)(?:social science(?:s)?|conseil(?:s)? recherche(?:s)?(?:\ssciences humaines)? canada|humanities\sresearch)).*", "utf_8")
    nrc_regex = unicode(".*(?i)(?:national research council|conseil(?:s)? national recherche(?:s)?).*", "utf_8")


    def VTiter(self, *parsedArgs, **envars):
        largs, dictargs = self.full_parse(parsedArgs)

        self.nonames = True
        self.names = []
        self.types = []

        if 'query' not in dictargs:
            raise functions.OperatorError(__name__.rsplit('.')[-1], "No query argument ")
        query = dictargs['query']

        are_all_canadians_approved = False
        cihr_unidentified = ''
        nserc_unidentified = ''
        sshrc_unidentified = ''
        nrc_unidentified = ''

        cur = envars['db'].cursor()
        c = None
        try:
            c = cur.execute(query)

            yield [('docid', 'text'), ('id', 'text'), ('textsnippet', 'text')]

            temp_table = []
            is_first_row = True
            for row in c:
                if is_first_row:  # Take all the funders' data, from the first row.
                    # The first 3 columns (0-2) are the table's data (id, temp_textsnippet, textsnippet)
                    cihr_unidentified = row[3]
                    nserc_unidentified = row[4]
                    sshrc_unidentified = row[5]
                    nrc_unidentified = row[6]
                    is_first_row = False
                    if cihr_unidentified and nserc_unidentified and sshrc_unidentified and nrc_unidentified:
                        are_all_canadians_approved = True
                    elif not cihr_unidentified and not nserc_unidentified and not sshrc_unidentified and not nrc_unidentified:
                        return  # Terminate if NO Canadian is selected.

                temp_table.append([row[0], "canadian_unspecified_id", row[1], row[2]]) # Store only the wanted table-data: docid, id, temp_textsnippet, textsnippet

        except Exception:
            try:
                raise
            finally:
                try:
                    if c:
                        c.close()
                except:
                    pass

        # After taking the table data, go search and update the table.
        prev_docid = ""
        docid_counter = 0

        for i in range(0, len(temp_table)):
            row = list(temp_table[i])  # The default rows of the table are "tuples", but we want lists.
            docid = row[0]
            id = row[1] # It is "canadian_unspecified_id" by default.
            temp_textsnippet = row[2]

            if docid != prev_docid: # If it is a new docid..
                docid_counter = 1
            else:
                docid_counter += 1
            prev_docid = docid

            related_with_funder_regex = False
            id_assigned = False

            if cihr_unidentified and regexprmatches(self.cihr_regex, temp_textsnippet):
                related_with_funder_regex = True
                if self.not_exists_diff_row_with_funder_id(temp_table, i, docid_counter, cihr_unidentified, temp_textsnippet):
                    id = cihr_unidentified
                    id_assigned = True

            if nserc_unidentified and not id_assigned and regexprmatches(self.nserc_regex, temp_textsnippet):
                related_with_funder_regex = True
                if self.not_exists_diff_row_with_funder_id(temp_table, i, docid_counter, nserc_unidentified, temp_textsnippet):
                    id = nserc_unidentified
                    id_assigned = True

            if sshrc_unidentified and not id_assigned and regexprmatches(self.sshrc_regex, temp_textsnippet):
                related_with_funder_regex = True
                if self.not_exists_diff_row_with_funder_id(temp_table, i, docid_counter, sshrc_unidentified, temp_textsnippet):
                    id = sshrc_unidentified
                    id_assigned = True

            if nrc_unidentified and not id_assigned and regexprmatches(self.nrc_regex, temp_textsnippet):
                related_with_funder_regex = True
                if self.not_exists_diff_row_with_funder_id(temp_table, i, docid_counter, nrc_unidentified, temp_textsnippet):
                    id = nrc_unidentified
                    id_assigned = True

            # else: it will be "canadian_unspecified_id".

            if not are_all_canadians_approved and id == "canadian_unspecified_id":
                continue    # Only allow the "canadian_unspecified_id" to pass through in case ALL the canadian funders are approved for mining. In which case we need to know if any "final categorization regex" failed.

            # Update the id in this row of the table.
            if id_assigned or not related_with_funder_regex:    # If we have an assigned funder-id or "canadian_unspecified_id" because of final regex mismatch.
                row[1] = id
                temp_table[i] = row
                final_row = list(row[0:2])  # Indexes: 0, 1 --> docid, id
                # The "id" might be "canadian_unspecified_id" in the future, as the regexes are expanded,
                # if a row is not identified as a canadian match, cause by a regex-mismatch which should be fixed.
                # row[2]: temp_textsnippet (we do not want it in the end)
                final_row.append(row[3])  # Add the final-text_snippet
                yield final_row
            #else: we have a similar record with the same grant-id as another record with the same docid.


    def not_exists_diff_row_with_funder_id(self, temp_table, i, docid_counter, funder_grantid, temp_textsnippet):
        # Iterate through the temp_table, for the past values that are related to this docid.
        # For example: for i = 56 and docid_counter = 3 we will search indexes: 54 and 55
        # from: (i-(docid_counter-1)) = (56-(3-1)) = (56-2) = 54, up to (i-1) = 55
        for j in range((i - (docid_counter-1)), i): # (i: exclusive) Avoid searching through other docids or future rows of the same docid which will have grant_id = "canadian_unspecified_id".
            row2 = list(temp_table[j])
            # We are sure that this row is related to the "docid" from the caller method.
            if row2[1] == funder_grantid and row2[2] != temp_textsnippet:
                return False
        return True


def Source():
    return vtbase.VTGenerator(CanadianVT)


if not ('.' in __name__):
    """
    This is needed to be able to test the function, put it at the end of every
    new function you create
    """
    import sys
    from functions import *

    testfunction()
    if __name__ == "__main__":
        reload(sys)
        sys.setdefaultencoding('utf-8')
        import doctest
        doctest.testmod()
