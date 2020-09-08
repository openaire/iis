"""

.. function:: slidingwindow(window) -> query results

Returns the query input results annotated with the window id as an extra column.
The window parameter defines the size of the window.

:Returned table schema:
    Same as input query schema.

Examples::

    >>> table1('''
    ... James   10
    ... Mark    7
    ... Lila    74
    ... Jane    44
    ... ''')
    >>> sql("slidingwindow window:2 select * from table1")
    wid | a     | b
    ----------------
    0   | James | 10
    1   | James | 10
    1   | Mark  | 7
    2   | Mark  | 7
    2   | Lila  | 74
    3   | Lila  | 74
    3   | Jane  | 44
    >>> sql("slidingwindow window:3 select * from table1")
    wid | a     | b
    ----------------
    0   | James | 10
    1   | James | 10
    1   | Mark  | 7
    2   | James | 10
    2   | Mark  | 7
    2   | Lila  | 74
    3   | Mark  | 7
    3   | Lila  | 74
    3   | Jane  | 44


"""

import setpath
import vtbase
import functions
import gc
from collections import deque

### Classic stream iterator
registered=True
       
class SlidingWindow(vtbase.VT):
    def VTiter(self, *parsedArgs,**envars):
        largs, dictargs = self.full_parse(parsedArgs)

        if 'query' not in dictargs:
            raise functions.OperatorError(__name__.rsplit('.')[-1],"No query argument ")
        query=dictargs['query']

        if 'window' not in dictargs:
            raise functions.OperatorError(__name__.rsplit('.')[-1],"No window argument ")

        cur=envars['db'].cursor()
        c=cur.execute(query, parse = False)

        try:
            yield [('wid','integer')] + list(cur.getdescriptionsafe())
        except StopIteration:
            try:
                raise
            finally:
                try:
                    c.close()
                except:
                    pass

        wid=0
        window=deque([], int(dictargs['window']))
        while True:
            window.append(c.next())
            for r in window:
                yield (wid,) + r
            wid+=1

def Source():
    return vtbase.VTGenerator(SlidingWindow)

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
