"""

.. function:: hidden(query) -> query results

Executes the query, without returning any of its rows.

:Returned table schema:
    Same as input query schema.

Examples::

    >>> table1('''
    ... James   10	2
    ... Mark    7	3
    ... Lila    74	1
    ... ''')
    >>> sql("hidden select * from table1")
    a | b | c
    ---------
    
    >>> sql("hidden select * from table1 order by c")
    a | b | c
    ---------

"""
import setpath
import vtbase
import functions

### Classic stream iterator
registered=True
       
class NopVT(vtbase.VT):
    def VTiter(self, *parsedArgs, **envars):
        largs, dictargs = self.full_parse(parsedArgs)

        if 'query' not in dictargs:
            raise functions.OperatorError(__name__.rsplit('.')[-1], "No query argument ")
        query = dictargs['query']

        c = envars['db'].cursor()
        q = c.execute(query, parse=False)

        try:
            yield list(c.getdescriptionsafe())
        except StopIteration:
            try:
                raise
            finally:
                try:
                    c.close()
                except:
                    pass

        while True:
            q.next()


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


