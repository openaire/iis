"""
.. function:: coltypes(query:None)

Returns the input query results column names and types.

:Returned table schema:
    - *column* text
        Column name of input query *schema*
    - *type* text
        Type of column

Examples:

    >>> sql("coltypes select 5 as vt")
    column | type
    -------------
    vt     | None

Applying coltypes in the result of virtual table func:`typing` function in the same query

    >>> sql("coltypes typing 'vt:int' select 5 as vt")
    column | type
    -------------
    vt     | int

.. doctest::
    :hide:

    >>> sql("select * from (coltypes typing 'text' select '10' ) as a, (coltypes typing 'int' select '10' ) as b where a.column=b.column")
    column | type | column | type
    -----------------------------
    '10'   | text | '10'   | int
"""

import setpath
import vtbase
import functions

registered=True

class ColTypes(vtbase.VT):
    def VTiter(self, *parsedArgs,**envars):
        largs, dictargs = self.full_parse(parsedArgs)

        if 'query' not in dictargs:
            raise functions.OperatorError(__name__.rsplit('.')[-1],"No query argument ")

        query=dictargs['query']
        connection=envars['db']

        yield (('column', 'text'), ('type', 'text'))

        cur=connection.cursor()
        execit=cur.execute(query, parse = False)
        try:
            samplerow=execit.next()
        except StopIteration:
            pass

        vals=cur.getdescriptionsafe()
        cur.close()

        for i in vals:
            yield i
        
def Source():
    return vtbase.VTGenerator(ColTypes)


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