"""

.. function:: ordered(query) -> query results

Ordered virtual table returns its data as they are. The main difference with unindexed, is that
it signals to the SQLite engine that the results are ordered in whatever order
SQLite prefers, so a possible group by on the results will happen incrementally.

:Returned table schema:
    Same as input query schema.

Examples::

    >>> table1('''
    ... James   10	2
    ... Mark    7	3
    ... Lila    74	1
    ... ''')

    The following query is calculated incrementally
    
    >>> sql("select a, count(*) from (ordered select * from table1) group by a")
    a     | count(*)
    ----------------
    James | 1
    Mark  | 1
    Lila  | 1
  
    >>> sql("select * from (ordered select * from table1) order by c")
    a     | b  | c
    --------------
    James | 10 | 2
    Mark  | 7  | 3
    Lila  | 74 | 1

    Notice that the order by does not work as it should because igroup has
    fooled the SQLite engine into believing that the order of the results are
    in the correct order (they aren't).

"""
import setpath
import vtbase
import functions
import gc

### Classic stream iterator
registered=True
       
class Ordered(vtbase.VT):
    def BestIndex(self, constraints, orderbys):
        return (None, 0, None, True, 1000)

    def VTiter(self, *parsedArgs,**envars):
        largs, dictargs = self.full_parse(parsedArgs)

        if 'query' not in dictargs:
            raise functions.OperatorError(__name__.rsplit('.')[-1],"No query argument ")
        query=dictargs['query']

        cur = envars['db'].cursor()
        q = cur.execute(query, parse=False)

        try:
            yield list(cur.getdescriptionsafe())
        except StopIteration:
            try:
                raise
            finally:
                try:
                    c.close()
                except:
                    pass

        gc.disable()
        while True:
            yield q.next()
        gc.enable()


def Source():
    return vtbase.VTGenerator(Ordered)

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


