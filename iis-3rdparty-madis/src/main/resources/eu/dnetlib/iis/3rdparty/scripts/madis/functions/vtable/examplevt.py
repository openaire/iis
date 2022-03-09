"""
.. function:: examplevt(arguments)

A minimal example of a virtual table. Returns all the arguments passed to it.

:Returned table schema:
    Column names start from C1... , all column types are text

Examples:

    >>> sql("select * from examplevt(1, '2', 'var3')")    # doctest:+ELLIPSIS
    varname          | value
    -------------------------------------------------------------
    parsedargs       | (u'1', u'2', u'var3')
    envar:tablename  | vt_773987998
    envar:modulename | examplevt
    ...
    envar:dbname     | temp

    >>> sql("select * from (examplevt 'var1' 'var2' v1:test select 5)")    # doctest:+ELLIPSIS
    varname          | value
    --------------------------------------------------------------------
    parsedargs       | (u'query:select 5', u'var1', u'var2', u'v1:test')
    envar:tablename  | vt_1975870853
    envar:modulename | examplevt
    ...
    envar:dbname     | temp

"""
from . import vtbase

registered=True
external_query = True

class examplevt(vtbase.VT):
    def VTiter(self, *parsedArgs, **envars):
        yield [('varname', ), ('value', 'text')]

        largs, dictargs = self.full_parse(parsedArgs)

        li = 0
        for i in largs:
            yield [li, str(i)]
            li += 1

        for k, v in dictargs.items():
            yield [str(k), str(v)]

        for x,y in envars.items():
            yield ["envar:"+x, str(y)]

def Source():
    return vtbase.VTGenerator(examplevt)

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
