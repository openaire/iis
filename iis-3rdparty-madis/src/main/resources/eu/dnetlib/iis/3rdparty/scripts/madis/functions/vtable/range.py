"""
.. function:: range([from:0,[to:10,[step:1]]])

Returns a range of integer numbers.

:Returned table schema:
    - *value* int
        Number in range.

.. note::

    The parameters can be given both named or unnamed. In unnamed mode parameter order is from,to,step.


Named parameters:

:from:
    Number to begin from. Default is 0
:to:
    Number to reach. Default is 10. The *to* number is not returned
:step:
    Step to augment the returned numbers. Default is 1

Examples::

    >>> sql("select * from range()")
    C1
    --
    0
    1
    2
    3
    4
    5
    6
    7
    8
    9
    
    >>> sql("select * from range('from:1','to:11')")
    C1
    --
    1
    2
    3
    4
    5
    6
    7
    8
    9
    10
    
    >>> sql("select * from range('from:2','to:15','step:3')")
    C1
    --
    2
    5
    8
    11
    14
    
    >>> sql("select * from range(1,10,2)")
    C1
    --
    1
    3
    5
    7
    9

    >>> sql("select * from range(5)")
    C1
    --
    1
    2
    3
    4
    5

"""

import vtbase
registered=True

class RangeVT(vtbase.VT):
    def VTiter(self, *parsedArgs,**envars):
        largs, dictargs = self.full_parse(parsedArgs)
        fromv=0
        tov=10
        stepv=1

        if 'from' in dictargs:
            fromv=int(dictargs['from'])
        if 'to' in dictargs:
            tov=int(dictargs['to'])
        if 'step' in dictargs:
            stepv=int(dictargs['step'])
        if len(largs)>=1:
            fromv=int(largs[0])
        if len(largs)>=2:
            tov=int(largs[1])
        if len(largs)>=3:
            stepv=int(largs[2])
        if len(largs)==1:
            fromv=1
            tov=int(largs[0])+1

        if 'query' in dictargs:
            fromv=1
            tov=int(dictargs['query'])+1

        yield [('C1', 'int')]

        for i in xrange(fromv,tov,stepv):
            yield [i]

def Source():
    return vtbase.VTGenerator(RangeVT)

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