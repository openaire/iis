"""
.. function:: stdinput() -> standard input stream

Returns the standard input stream

:Returned table schema:
    One column automatically named C1.

Examples::
    >>> sql("select * from stdinput()")
    c1
    -------------
    stdinputline1
    stdinputline2
    stdinputline3
"""
import sys
import vtbase

registered=True

class StdInput(vtbase.VT):
    def VTiter(self, *args,**formatArgs):
        yield [('C1', 'text')]

        while True:
            a = sys.stdin.readline()
            if not a:
                break
            yield (unicode(a.rstrip('\r\n'), 'utf_8'),)

def Source():
    return vtbase.VTGenerator(StdInput)

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


