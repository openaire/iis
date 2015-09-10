"""
.. function:: expand(query:None)

Executes the input query and returns the result expanding any multiset values returned. The returned result is produced iteratively.

:Returned table schema:
    Same as input query schema expanded with multiset functions column naming. When *as* renaming function is used at a multiset function,
    if the multiset function returns only one column it is named according to the *as* value,
    else a positive integer (1,2...n) is appended to the column name indicating column index in the multiset function result.

Examples::

    >>> table1('''
    ... James   10	2
    ... Mark    7	3
    ... Lila    74	1
    ... ''')
    >>> sql("select ontop(1,c,a,b) from table1")
    top1 | top2
    -----------
    Mark | 7
    >>> sql("select ontop(1,c,b,c) as prefs from table1")
    prefs1 | prefs2
    ---------------
    7      | 3
    >>> sql("select ontop(1,c,a) as nameontop from table1")
    nameontop
    ---------
    Mark
    
    The explicit invocation of expand function won't affect the output since it is already automatically invoked because of the multiset function ontop.
    
    >>> sql("expand expand select ontop(2,b,a) from table1")
    top1
    -----
    Lila
    James

.. doctest::
    :hide:
        
    >>> table2('''
    ... Fibi    40
    ... Monika  5
    ... Soula   17
    ... ''')
    >>> sql("select * from (select ontop(1,c,a,b) from table1) as a,(select ontop(1,c,a,b) from table1) as b,(select ontop(2,b,a,b) from table2) as c where a.top2=b.top2 and a.top2<c.top2")
    top1 | top2 | top1 | top2 | top1  | top2
    ----------------------------------------
    Mark | 7    | Mark | 7    | Fibi  | 40
    Mark | 7    | Mark | 7    | Soula | 17
    
    >>> sql("select * from (select ontop(3,c,a,b) from table1) as a,(select ontop(3,c,a,b) from table1) as b,(select ontop(2,b,a,b) from table2) as c")
    top1  | top2 | top1  | top2 | top1  | top2
    ------------------------------------------
    Mark  | 7    | Mark  | 7    | Fibi  | 40
    Mark  | 7    | Mark  | 7    | Soula | 17
    Mark  | 7    | James | 10   | Fibi  | 40
    Mark  | 7    | James | 10   | Soula | 17
    Mark  | 7    | Lila  | 74   | Fibi  | 40
    Mark  | 7    | Lila  | 74   | Soula | 17
    James | 10   | Mark  | 7    | Fibi  | 40
    James | 10   | Mark  | 7    | Soula | 17
    James | 10   | James | 10   | Fibi  | 40
    James | 10   | James | 10   | Soula | 17
    James | 10   | Lila  | 74   | Fibi  | 40
    James | 10   | Lila  | 74   | Soula | 17
    Lila  | 74   | Mark  | 7    | Fibi  | 40
    Lila  | 74   | Mark  | 7    | Soula | 17
    Lila  | 74   | James | 10   | Fibi  | 40
    Lila  | 74   | James | 10   | Soula | 17
    Lila  | 74   | Lila  | 74   | Fibi  | 40
    Lila  | 74   | Lila  | 74   | Soula | 17
"""

import setpath
import vtbase
import functions
import re
from lib.sqlitetypes import getElementSqliteType

### Classic stream iterator
registered = True

noas = re.compile('.*\(.*\).*')

def izip2(*args):
    # izip_longest('ABCD', 'xy', fillvalue='-') --> Ax By C- D-
    counter = sum((1 if type(x) is generator else 0 for x in args))
    iterators = [chain(it, sentinel(), fillers) for it in args]
    try:
        while iterators:
            yield tuple(map(next, iterators))
    except ZipExhausted:
        pass

class Expand(vtbase.VT):
    def VTiter(self, *parsedArgs,**envars):

        def exprown(row):
            for i in xrange(len(row)):
                iobj = row[i]
                if type(iobj) is tuple:
                    for el in iobj[1]:
                        for l in exprown(row[(i+1):]):
                            yield row[:i] + list(el) + l
                    try:
                        del(self.connection.openiters[iobj[0]])
                    except KeyboardInterrupt:
                        raise
                    except:
                        pass
                    return

            yield row

        largs, dictargs = self.full_parse(parsedArgs)

        if 'query' not in dictargs:
            raise functions.OperatorError(__name__.rsplit('.')[-1],"No query argument ")
        query=dictargs['query']

        self.connection = envars['db']
        oiters = self.connection.openiters
        iterheader = functions.iterheader
        lenIH = len(iterheader)

        cur = self.connection.cursor()
        c= cur.execute(query, parse = False)

        schema = cur.getdescriptionsafe()
        self.nonames = True
        types = []
        orignames = [x[0] for x in schema]
        origtypes = [x[1] if len(x)>1 else 'None' for x in schema]

        nrow = []
        nnames = []
        ttypes=[]

        try:
            row = c.next()
        except StopIteration:
            yield schema
            return

        rowlen = len(row)

        for i in xrange(rowlen):
            obj = row[i]
            if type(obj) is buffer and obj[:lenIH] == iterheader:
                strobj = str(obj)
                oiter=oiters[strobj]
                try:
                    first = oiter.next()
                except StopIteration:
                    first = [None]

                ttypes += ['GUESS']*len(first)
                if noas.match(orignames[i]):
                    badschema = False
                    if type(first) != tuple:
                        badschema = True

                    for i in first:
                        if type(first) != tuple or type(i) not in (unicode, str) or i is None:
                            badschema = True
                            break
                            
                    if badschema:
                        raise functions.OperatorError(__name__.rsplit('.')[-1],
                            "First yielded row of multirow functions, should contain the schema inside a Python tuple.\nExample:\n  yield ('C1', 'C2')")

                    nnames += list(first)
                else:
                    if len(first) == 1:
                        nnames += [orignames[i]]
                    else:
                        nnames += [orignames[i]+str(j) for j in xrange(1, len(first)+1)]
                nrow += [(strobj, oiter)]
            else:
                ttypes += [origtypes[i]]
                nnames += [orignames[i]]
                nrow += [obj]

        firstbatch = exprown(nrow)
        try:
            firstrow = firstbatch.next()
        except StopIteration:
            firstrow = None

        for i, v in enumerate(ttypes):
            if v == 'GUESS':
                try:
                    v = getElementSqliteType(firstrow[i])
                except Exception, e:
                    v = 'text'
            types.append(v)

        yield [(nnames[i], types[i]) for i in xrange(len(types))]

        if firstrow is not None:
            yield firstrow
            
        for exp in firstbatch:
            yield exp

#        lastvals = [None] * len(nrow)
        for row in c:
            nrow = list(row)
#            itercount = 0

            for i in xrange(rowlen):
                if type(nrow[i]) is buffer and nrow[i][:lenIH] == iterheader:
                    striter = str(nrow[i])
                    oiter = oiters[striter]
                    oiter.next()
                    nrow[i] = (striter, oiter)
#                    itercount += 1

            for exp in exprown(nrow):
                yield exp

#            if itercount > 0:
#                while True:
#                    n = 0
#                    irow = []
#                    for i in xrange(len(row)):
#                        val = nrow[i]
#                        if type(val) == tuple:
#                            try:
#                                ival = val[1].next()
#                                lastvals[i] = ival
#                            except StopIteration:
#                                ival = lastvals[i]
#                                itercount -= 1
#                            irow += nrow[n:i]
#                            n = i+1
#                            irow += ival
#
#                    irow += nrow[n:]
#                    if itercount == 0:
#                        break
#                    else:
#                        yield irow
#            else:
#                yield row



def Source():
    return vtbase.VTGenerator(Expand)

if not ('.' in __name__):
    """
    This is needed to be able to test the function, put it at the end of every
    new function you create
    """
    import sys
    import setpath
    from functions import *
    testfunction()
    if __name__ == "__main__":
        reload(sys)
        sys.setdefaultencoding('utf-8')
        import doctest
        doctest.testmod()


