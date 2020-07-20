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
import vtbase

registered=True

class examplevt(vtbase.VT):
    def VTiter(self, *parsedArgs, **envars):
        yield [('varname', 'text'), ('value', 'text')]

        yield ["parsedargs", unicode(parsedArgs)]

        for x,y in envars.iteritems():
            yield ["envar:"+x, str(y)]

def Source():
    return vtbase.VTGenerator(examplevt)

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
