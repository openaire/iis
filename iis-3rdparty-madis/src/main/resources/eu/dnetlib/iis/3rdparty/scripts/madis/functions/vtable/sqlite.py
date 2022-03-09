"""
.. function:: sqlite(dbfilename, query:None)

Connects to an SQLite DB and returns the results of query.

Examples:

    >>> sql("select * from (sqlite 'testdb.db' select 5 as num, 'test' as text);")
    num | text
    -----------
    5   | test

"""

from . import setpath
from . import vtbase
import functions
import os

registered=True
external_query = True

class SQLite(vtbase.VT):
    def VTiter(self, *parsedArgs,**envars):
        largs, dictargs = self.full_parse(parsedArgs)

        if 'query' not in dictargs:
            raise functions.OperatorError(__name__.rsplit('.')[-1],"No query argument ")

        query=dictargs['query']

        if len(largs) > 0:
            sqdb = largs[0]

        if 'db' in dictargs:
            sqdb = dictargs['db']

        sqdb = str(os.path.abspath(os.path.expandvars(os.path.expanduser(os.path.normcase(sqdb)))))
        conn = functions.Connection(sqdb)

        cur = conn.cursor()
        cur.execute(query)

        yield cur.getdescriptionsafe()

        while True:
            yield next(cur)

        cur.close()
        
def Source():
    return vtbase.VTGenerator(SQLite)


if not ('.' in __name__):
    """
    This is needed to be able to test the function, put it at the end of every
    new function you create
    """
    import sys
    from . import setpath
    from functions import *
    testfunction()
    if __name__ == "__main__":
        reload(sys)
        sys.setdefaultencoding('utf-8')
        import doctest
        doctest.testmod()