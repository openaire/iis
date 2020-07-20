# coding: utf-8

import setpath
import sqlparse.sql
import sqlparse
import re
from sqlparse.tokens import *
import zlib
import functions

try:
    from collections import OrderedDict
except ImportError:
    # Python 2.6
    from lib.collections26 import OrderedDict

break_inversion_subquery = re.compile(r"""\s*((?:(?:(?:'[^']*?'|\w+:[^\s]+)\s*)*))((?i)of\s|from\s|)(.*?)\s*$""",
                                      re.DOTALL | re.UNICODE)
find_parenthesis = re.compile(r"""\s*\((.*)\)\s*$""", re.DOTALL | re.UNICODE)
viewdetector = re.compile(r'(?i)\s*create\s+(?:temp|temporary)\s+view\s+', re.DOTALL | re.UNICODE)
inlineop = re.compile(r'\s*/\*\*+[\s\n]*((?:def\s+|class\s+).+)[^*]\*\*+/', re.DOTALL | re.UNICODE)

_statement_cache = OrderedDict()
_statement_cache_size = 1000

# delete reserved SQL keywords that collide with our vtables
if __name__ != "__main__":
    for i in ['EXECUTE', 'NAMES', 'CACHE', 'EXEC', 'OUTPUT']:
        if i in sqlparse.keywords.KEYWORDS:
            del sqlparse.keywords.KEYWORDS[i]


#Parse comments for inline ops
def opcomments(s):
    if r'/**' not in unicode(s):
        return []

    out = []
    constr_comm = None
    for i in s.tokens:
        ui = unicode(i)
        if type(i) == sqlparse.sql.Comment:
            op = inlineop.match(ui)
            if op != None:
                out.append(op.groups()[0])

        # Construct comment to work around sqlparse bug
        if constr_comm is not None:
            constr_comm += ui

        if type(i) == sqlparse.sql.Token:
            if ui == u'/*':
                if constr_comm is not None:
                    constr_comm = None
                else:
                    constr_comm = u'/*'
            elif ui == u'*/':
                if constr_comm is not None:
                    op = inlineop.match(constr_comm)
                    if op != None:
                        out.append(op.groups()[0])
                    constr_comm = None
    return out


#Top level transform (runs once)
def transform(query, multiset_functions=None, vtables=[], row_functions=[], substitute=lambda x: x):
    if type(query) not in (str, unicode):
        return (query, [], [])

    s = query
    subsquery = substitute(s)

    # Check cache
    if subsquery in _statement_cache:
        return _statement_cache[subsquery]

    enableInlineops = False
    if r'/**' in subsquery:
        enableInlineops = True

    out_vtables = []

    st = sqlparse.parse(subsquery)

    trans = Transclass(multiset_functions, vtables, row_functions)
    s_out = ''
    sqp = ('', [], [])
    inlineops = []

    for s in st:
        # delete question mark
        strs = re.match(r"(.*?);*\s*$", unicode(s), re.DOTALL | re.UNICODE).groups()[0]
        st1 = sqlparse.parse(strs)
        if len(st1) > 0:
            if enableInlineops:
                inlineops.append(opcomments(st1[0]))
            sqp = trans.rectransform(st1[0])
            strs = unicode(sqp[0])
            s_out += strs
            s_out += ';'

            # Detect create temp view and mark its vtables as permanent
            if viewdetector.match(strs):
                out_vtables += [x + (False,) for x in sqp[1]]
            else:
                out_vtables += sqp[1]

    result = (s_out, vt_distinct(out_vtables), sqp[2], inlineops)

    if len(_statement_cache) < _statement_cache_size:
        _statement_cache[subsquery] = result
    else:
        _statement_cache.popitem(last=False)
        _statement_cache[subsquery] = result
    return result


class Transclass:
    direct_exec = []
    multiset_functions = None
    vtables = []
    row_functions = []

    def __init__(self, multiset_functions=None, vtables=[], row_functions=[]):
        self.direct_exec = []
        self.multiset_functions = multiset_functions
        self.vtables = vtables
        self.row_functions = row_functions

    #recursive transform
    def rectransform(self, s, s_orig=None):
        if not (re.search(ur'(?i)(select|' + '|'.join([x for x in self.vtables]) + '|' + '|'.join(
                self.multiset_functions) + '|' + '|'.join(self.row_functions) + ')', unicode(s), re.UNICODE)):
            return unicode(s), [], self.direct_exec

        out_vtables = []

        if s_orig is None:
            s_orig = s

        query = None

        #Expand functions with spaces between them and their parenthesis
        for t in s_orig.tokens:
            tfm = re.match('(\w+)\s\(', unicode(t), re.UNICODE)
            if isinstance(t, sqlparse.sql.Function) and tfm and (
                    tfm.groups()[0] in self.vtables or tfm.groups()[0] in self.row_functions):
                tidx = s_orig.token_index(t)
                s_orig.tokens[tidx:tidx + 1] = t.tokens

        fs = [x for x in expand_tokens(s)]

        # Process internal parenthesis
        for t in fs:
            if type(t) is sqlparse.sql.Parenthesis:
                subq = find_parenthesis.match(unicode(t))
                if subq != None:
                    subq = subq.groups()[0]
                    t.tokens = sqlparse.parse(subq)[0].tokens
                    out_vtables += self.rectransform(t)[1]
                    t.tokens[0:0] = [sqlparse.sql.Token(Token.Punctuation, '(')]
                    t.tokens.append(sqlparse.sql.Token(Token.Punctuation, ')'))

        # Process Inversions

        #Process direct row inversion
        t = re.match(r'\s*(\w+)(\s+.*|$)', unicode(s), re.DOTALL | re.UNICODE)
        if t != None and t.groups()[0].lower() in self.row_functions:
            op_for_inv = t.groups()[0]
            rest = t.groups()[1]
            params, preposition, subq = break_inversion_subquery.match(rest).groups()
            paramslist = [format_param(''.join(x)) for x in re.findall(r"'([^']*?)'|(\w+:[^\s]+)", params, re.UNICODE)]
            if subq != '':
                if len(preposition) > 0:
                    subq, v, dv = self.rectransform(sqlparse.parse(subq)[0])
                    out_vtables += v
                    paramslist += ['(' + subq + ')']
                else:
                    paramslist += [format_param(subq)]
            inv_s = 'SELECT ' + op_for_inv + '(' + ','.join(paramslist) + ')'
            subs = sqlparse.parse(inv_s)[0]
            s_orig.tokens[s_orig.token_index(s.tokens[0]):s_orig.token_index(s.tokens[-1]) + 1] = subs.tokens
            s = subs

        fs = [x for x in expand_tokens(s)]

        #Proccess vtable inversion
        for t in fs:
            if t.ttype == Token.Keyword.DML:
                break
            strt = unicode(t).lower()
            if strt in self.vtables:
                #print "FOUND INVERSION:", strt, fs
                tindex = fs.index(t)
                # Break if '.' exists before vtable
                if tindex > 0 and unicode(fs[tindex - 1]) == '.':
                    break
                op_for_inv = strt
                try:
                    rest = ''.join([unicode(x) for x in fs[tindex + 1:]])
                except KeyboardInterrupt:
                    raise
                except:
                    rest = ''
                params, preposition, subq = break_inversion_subquery.match(rest).groups()
                orig_subq = subq
                if subq != '':
                    subq, v, dv = self.rectransform(sqlparse.parse(subq)[0])
                    out_vtables += v
                if not hasattr(self.vtables[strt], 'external_stream'):
                    if subq != '':
                        paramslist = [format_query(subq)]
                    else:
                        paramslist = []
                    paramslist += [format_param(''.join(x)) for x in
                                   re.findall(r"'([^']*?)'|(\w+:[^\s]+)", params, re.UNICODE)]
                    inv_s = ''.join(
                        [unicode(x) for x in fs[:fs.index(t)]]) + 'SELECT * FROM ' + op_for_inv + '(' + ','.join(
                            paramslist) + ')'
                else:
                    paramslist = [format_param(''.join(x)) for x in
                                  re.findall(r"'([^']*?)'|(\w+:[^\s]+)", params, re.UNICODE)]
                    inv_s = ''.join(
                        [unicode(x) for x in fs[:fs.index(t)]]) + 'SELECT * FROM ' + op_for_inv + '(' + ','.join(
                            paramslist) + ') ' + subq
                subs = sqlparse.parse(inv_s)[0]
                self.direct_exec += [(op_for_inv, paramslist, orig_subq)]
                #print self.direct_exec
                s_orig.tokens[s_orig.token_index(s.tokens[0]):s_orig.token_index(s.tokens[-1]) + 1] = subs.tokens
                s = subs
                break

        # find first select
        s_start = s.token_next_match(0, Token.Keyword.DML, r'(?i)select', True)
        if s_start is not None:
            # find keyword that ends substatement
            s_end = s.token_next_match(s.token_index(s_start), Token.Keyword, (
                r'(?i)union', r'(?i)order', r'(?i)limit', r'(?i)intersect', r'(?i)except', r'(?i)having'), True)
            if len(s.tokens) < 3:
                return unicode(s), vt_distinct(out_vtables), self.direct_exec
            if s_end is None:
                if s.tokens[-1].value == ')':
                    s_end = s.tokens[-2]
                else:
                    s_end = s.tokens[-1]
            else:
                if s.token_index(s_end) + 1 >= len(s.tokens):
                    raise functions.MadisError("'" + unicode(s_end).upper() + "' should be followed by something")
                out_vtables += self.rectransform(
                    sqlparse.sql.Statement(s.tokens_between(s.tokens[s.token_index(s_end) + 1], s.tokens[-1])), s)[1]
                s_end = s.tokens[s.token_index(s_end) - 1]
            query = sqlparse.sql.Statement(s.tokens_between(s_start, s_end))
        else:
            return unicode(s), vt_distinct(out_vtables), self.direct_exec

        # find from and select_parameters range
        from_range = None
        from_start = query.token_next_match(0, Token.Keyword, r'(?i)from', True)

        # process virtual tables in from range
        if from_start is not None:
            from_end = query.token_next_by_instance(query.token_index(from_start), sqlparse.sql.Where)
            if from_start == query.tokens[-1]:
                raise functions.MadisError("Error in FROM range of: '" + str(query) + "'")
            if from_end is None:
                from_end = query.tokens[-1]
                from_range = sqlparse.sql.Statement(
                    query.tokens_between(query.tokens[query.token_index(from_start) + 1], from_end))
            else:
                from_range = sqlparse.sql.Statement(
                    query.tokens_between(query.tokens[query.token_index(from_start) + 1], from_end, exclude_end=True))
            for t in [x for x in expand_type(from_range, (sqlparse.sql.Identifier, sqlparse.sql.IdentifierList))]:
                if unicode(t).lower() in ('group', 'order'):
                    break
                if type(t) is sqlparse.sql.Function:
                    vname = vt_name(unicode(t))
                    fname = t.tokens[0].get_real_name().lower()
                    if fname in self.vtables:
                        out_vtables += [(vname, fname, unicode(t.tokens[1])[1:-1])]
                        t.tokens = [sqlparse.sql.Token(Token.Keyword, vname)]
                    else:
                        raise functions.MadisError("Virtual table '" + fname + "' does not exist")

        if from_start is not None:
            select_range = sqlparse.sql.Statement(query.tokens_between(query.tokens[1], from_start, exclude_end=True))
        else:
            select_range = sqlparse.sql.Statement(query.tokens_between(query.tokens[1], query.tokens[-1]))

        # Process EXPAND functions
        for t in flatten_with_type(select_range, sqlparse.sql.Function):
            if hasattr(t.tokens[0], 'get_real_name'):
                fname = t.tokens[0].get_real_name()
            else:
                fname = unicode(t.tokens[0])
            fname = fname.lower().strip()
            if fname in self.multiset_functions:
                t = s_orig.group_tokens(sqlparse.sql.Parenthesis, s_orig.tokens_between(s_start, s_end))
                vname = vt_name(unicode(t))
                out_vtables += [(vname, 'expand', format_query(t))]
                s_orig.tokens[s_orig.token_index(t)] = sqlparse.sql.Token(Token.Keyword, 'select * from ' + vname + ' ')
                break

        return unicode(s), vt_distinct(out_vtables), self.direct_exec


def vt_name(s):
    tmp = re.sub(r'([^\w])', '_', 'vt_' + unicode(zlib.crc32(s.encode('utf-8'))), re.UNICODE)
    return re.sub(r'_+', '_', tmp, re.UNICODE)


def format_query(s):
    q = "'query:" + unicode(s).replace("'", "''") + "'"
    q = q.replace('\n', ' ')
    return q


def format_param(s):
    return "'" + unicode(s).replace("'", "''") + "'"


def format_identifiers(s):
    return unicode(s).replace(' ', '').replace('\t', '')


def flatten_with_type(inpt, clss):
    """Generator yielding ungrouped tokens.

    This method is recursively called for all child tokens.
    """
    for token in inpt.tokens:
        if isinstance(token, clss):
            yield token
        else:
            if token.is_group() or type(token) is sqlparse.sql.Parenthesis:
                for i in flatten_with_type(token, clss):
                    yield i


def expand_type(inpt, clss):
    """Generator yielding ungrouped tokens.

    This method is recursively called for all child tokens.
    """
    for token in inpt.tokens:
        if token.is_group() and isinstance(token, clss):
            for i in expand_type(token, clss):
                yield i
        else:
            yield token


def expand_tokens(inpt):
    """Generator yielding ungrouped tokens.

    This method is recursively called for all child tokens.
    """
    for token in inpt.tokens:
        if (token.is_group() and isinstance(token, (
        sqlparse.sql.Identifier, sqlparse.sql.IdentifierList, sqlparse.sql.Where))):
            for i in expand_tokens(token):
                yield i
        else:
            yield token


def vt_distinct(vt):
    vtout = OrderedDict()
    for i in vt:
        if i[0] not in vtout:
            vtout[i[0]] = i
        else:
            if not vtout[i[0]][-1] == False:
                vtout[i[0]] = i

    return vtout.values()


if __name__ == "__main__":

    sql = []
    multiset_functions = ['nnfunc1', 'nnfunc2', 'apriori', 'ontop', 'strsplit']

    def file():
        pass

    file.external_stream = True

    def execv():
        pass

    execv.no_results = True

    vtables = {'file': file, 'lnf': True, 'funlalakis': True, 'filela': True, 'sendto': True, 'helpvt': True,
               'output': True, 'names': True, 'cache': True, 'testvt': True, 'exec': execv, 'flow': True}
    row_functions = ['help', 'set', 'execute', 'var', 'toggle', 'strsplit', 'min', 'ifthenelse', 'keywords']

    sql += ["select a,b,(apriori(a,b,c,'fala:a')) from lalatable"]
    sql += ["create table a from select a,b,(apriori(a,b,c,'fala:a')) from lalatable, lala14, lala15"]
    sql += ["create table a from select a,b,(apriori(a,b,c,'fala:a')) from lalatable, lala14, lala15"]
    sql += ["select a,b,(apriori(a,b,c,'fala:a')) from lalatable where a=15 and b=23 and c=(1234)"]
    sql += ["select a,b,(apriori(a,b,c,'fala:a')) from lalatable where a=15 and b=23 and c=(1234) group by a order by"]
    sql += [
        "select a,b,(apriori(a,b,c,'fala:a')) from ('asdfadsf') where a=15 and b=23 and c=(1234) group by a order by"]
    sql += [
        "select a,b,(apriori(a,b,c,'fala:a')) from ('asdfadsf') where a=15 and b=23 and c=(1234) group by a order by b union select a,b from funlalakis('1234'), (select a from lnf('1234') )"]
    sql += ["select c1,c2 from file('test.tsv', 'param1');select a from filela('test.tsv') group by la"]
    sql += ['insert into la values(1,2,3,4)']
    sql += ["select apriori(a) from (select apriori('b') from table2)"]
    sql += [
        "select userid, top1, top2 from (select userid,ontop(3,preference,collid,preference) from colpreferences group by userid)order by top2 ; "]
    sql += ["select ontop(a), apriori(b) from lala"]
    sql += ["select ontop(a) from (select apriori(b) from table) order by a"]
    sql += [
        "select userid,ontop(3,preference,collid,preference),ontop(1,preference,collid) from colpreferences group by userid;"]
    sql += ["create table lala as select apriori(a) from table;"]
    sql += [
        "create table lila as select userid,ontop(3,preference,collid,preference),ontop(1,preference,collid) from colpreferences group by userid; "]
    sql += ["select * from file(test.txt)"]
    sql += ["select sum(b) from test_table group by a pivot b,c"]
    sql += ["select * from (helpvt lala)"]
    sql += ["output 'list'"]
    sql += ["(help lala)"]
    sql += [r"select * from tab1 union help 'lala'"]
    sql += [r"select * from file('list'),(select * from file('list'))"]
    sql += [r"create table ta as help list"]
    sql += [r"select * from (help lala)"]
    sql += [r"output 'lala' select apriori(a,b) from extable"]
    sql += [r"select apriori(a,b) from extable"]
    sql += [r"select * from file('/lala','param1:t')"]
    sql += [r"output '/lala' 'param1' select * from tab"]
    sql += [r"select apriori(a,b) from file(/lala/lalakis)"]
    sql += ["(select a from (sendto 'fileout.lala' 'tsv' select * from file('file.lala')))"]
    sql += ["sendto 'lala1' sendto 'fileout.lala' 'tsv' select * from file('file.lala'))"]
    sql += ["help 'lala'"]
    sql += ["names file 'lala'; helpvt lala"]
    sql += [r"select * from file() as a, file() as b;"]
    sql += [r"select file from (file 'alla) as lala"]
    sql += [r"  .help select * from file('lsls')"]
    sql += [r"  .execute select * from file('lsls')"]
    sql += [r"limit 1"]
    sql += [r"file 'lala'"]
    sql += [r"select * from lala union file 'lala' union file 'lala'"]
    sql += [r"file 'lala' limit 1"]
    sql += [r"create table lala file 'lala'"]
    sql += [r"SELECT * FROM (file 'lala')"]
    sql += [r"(file 'lala') union (file 'lala1')"]
    sql += [r"select (5+5) from (file 'lala1')"]
    sql += [
        r"select * from ( output 'bla' select * from file('collection-general.csv','dialect:line') where rowid!=1 ) "]
    sql += [r"select * from testtable where x not in (file 'lalakis')"]
    #sql+=[r".help ασδαδδ"]
    sql += [r"names (file 'testfile')"]
    #sql+=[r"select * from (select lala from table limit)"]
    sql += [r"""create table session_to_country(
	sesid text NOT NULL primary key,
	geoip_ccode text
);    """]
    sql += [r"""create table ip_country as select iplong,CC from (cache select cast(C3 as integer) as ipfrom,cast(C4 as
integer) as ipto, C5 as CC from file('file:GeoIPCountryCSV_09_2007.zip','compression:t','dialect:csv') ),tmpdistlong
where iplong>=ipfrom and iplong <=ipto;
"""]
    sql += [r"cache select * from lala;"]
    sql += [r"var 'lala' from var 'lala1'"]
    sql += [r"toggle tracing"]
    sql += [r"select strsplit('8,9','dialect:csv')"]
    sql += [r"testvt"]
    sql += [r"select date('now')"]
    sql += [r"exec select * from lala"]
    sql += [r"var 'usercc' from select min(grade) from (testvt) where grade>5;"]
    sql += [r"var 'usercc' from select 5;"]
    sql += [r"(exec flow file 'lala' 'lala1' asdfasdf:asdfdsaf);"]
    sql += [
        r"UPDATE merged_similarity SET  merged_similarity = ((ifthenelse(colsim,colsim,0)*0.3)+(ifthenelse(colsim,colsim,0)*0.3))"]
    sql += [r"toggle tracing ;"]
    sql += [r"select sesid, query from tac group by sesid having keywords('query')='lala'"]
    sql += [
        r"select sesid, query from tac group by sesid having keywords('query')='lala' union select * from file('lala')"]
    sql += [r"select * from (select 5 as a) where a=4 or (a=5 and a not in (select 3));"]
    sql += [r"select * from a where ((a.r in (select c1 from f)));"]
    sql += [r"select upper(a.output) from a"]
    sql += [r"select upper(execute) from a"]
    sql += [r"exec select a.5 from (flow file 'lala')"]
    sql += [r"select max( (select 5))"]
    sql += [r"cache select 5; create temp view as cache select 7; cache select 7"]
    sql += [r"select * from /** def lala(x): pass **/ tab"]
    sql += [r"select * from /* def lala(x): pass **/ tab"]
    sql += [r"/** def lala():return 6 **/ \n"]
    sql += [r"/** def lala():return 6 **/ "]

    for s in sql:
        print "====== " + unicode(s) + " ==========="
        a = transform(s, multiset_functions, vtables, row_functions)
        print "Query In:", s
        print "Query Out:", a[0].encode('utf-8')
        print "Vtables:", a[1]
        print "Direct exec:", a[2]
        print "Inline Ops:", a[3]