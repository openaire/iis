"""

.. function:: unindexed(query) -> query results

Returns the query input results without any change. UNINDEXED can be used as a
barrier for SQLite's optimizer, for debugging etc.

:Returned table schema:
    Same as input query schema.

Examples::

    >>> table1('''
    ... James   10	2
    ... Mark    7	3
    ... Lila    74	1
    ... ''')
    >>> sql("unindexed select * from table1")
    a     | b  | c
    --------------
    James | 10 | 2
    Mark  | 7  | 3
    Lila  | 74 | 1
    
    >>> sql("unindexed select * from table1 order by c")
    a     | b  | c
    --------------
    Lila  | 74 | 1
    James | 10 | 2
    Mark  | 7  | 3

    Note the difference with rowid table column.

"""
import setpath
import vtbase
import functions
import gc

### Classic stream iterator
registered=True
       
class NopVT(vtbase.VT):
    def VTiter(self, *parsedArgs,**envars):
        largs, dictargs = self.full_parse(parsedArgs)

        if 'query' not in dictargs:
            raise functions.OperatorError(__name__.rsplit('.')[-1],"No query argument ")
        query=dictargs['query']

        cur=envars['db'].cursor()
        c=cur.execute(query, parse = False)

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
            yield c.next()
        gc.enable()

def Source():
    return vtbase.VTGenerator(NopVT)

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


