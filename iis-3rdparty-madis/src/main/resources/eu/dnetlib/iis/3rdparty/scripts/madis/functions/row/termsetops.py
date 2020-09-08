# coding: utf-8

import itertools

def tset(*args):

    """
    .. function:: termsetdiff(termset1, termset2) -> termset

    Returns the termset that is the difference of sets of termset1 - termset2.

    Examples:

    >>> table1('''
    ... 't1 t2 t3' 't2 t3'
    ... 't3 t2 t1' 't3 t4'
    ... ''')
    >>> sql("select tset(a,b) from table1")
    tset(a,b)
    -----------
    t1 t2 t3
    t1 t2 t3 t4
    """

    return ' '.join(sorted(set(' '.join(args).split(' '))))

tset.registered=True

def tsetdiff(*args):

    """
    .. function:: termsetdiff(termset1, termset2) -> termset

    Returns the termset that is the difference of sets of termset1 - termset2.

    Examples:

    >>> table1('''
    ... 't1 t2 t3' 't2 t3'
    ... 't3 t2 t1' 't3 t4'
    ... ''')
    >>> sql("select tsetdiff(a,b) from table1")
    tsetdiff(a,b)
    -------------
    t1
    t1 t2
    """

    if len(args)<2:
        raise functions.OperatorError("tsetdiff","tsetdiff operator: at least two termsets should be provided")

    return ' '.join(sorted(set(args[0].split(' '))-set(args[1].split(' '))))

tsetdiff.registered=True

def tsetcombinations(*args):
    """
    .. function:: tsetcombinations(termset, r) -> termset

    Returns all the termset combinations of length r.
    It is a multiset operator that returns one column but many rows.

    .. seealso::

        * :ref:`tutmultiset` functions


    >>> sql("select tsetcombinations('t1 t2 t3 t4',2)")
    C1
    -----
    t1 t2
    t1 t3
    t1 t4
    t2 t3
    t2 t4
    t3 t4
    """
    if len(args)<1:
        raise functions.OperatorError("tsetcombinations","tsetcombinations operator: no input")

    tset=args[0]

    if not isinstance(args[1], int):
        raise functions.OperatorError("tsetcombinations","tsetcombinations operator: second argument should be integer")

    yield ("C1",)

    for p in itertools.combinations(sorted(tset.split(' ')), args[1]):
        first=False
        yield [' '.join(p)]

    if first:
        yield ['']

tsetcombinations.registered=True
tsetcombinations.multiset=True

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
