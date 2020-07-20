import random
# coding: utf-8
import math

def randomrange(*args):

    """
    .. function:: randomrange(start, end, step) -> int

    Returns a random number in the defined range

    Examples:

    >>> sql("select randomrange(0, 68, 1)") # doctest: +ELLIPSIS
    randomrange(0, 68, 1)
    ---------------------
    ...

    """

    try:
        ret=random.randrange(args[0],args[1],args[2])
    except ValueError:
        return None

    return ret

randomrange.registered=True

def gaussdistribution(*args):

    """
    .. function:: gaussdistribution(mean, sigma) -> float

    Returns a gaussian distribution. Sigma is the standard deviation of the
    distribution

    Examples:

    >>> sql("select gaussdistribution(10,5)") # doctest: +ELLIPSIS
    gaussdistribution(10,5)
    -----------------------
    ...

    """

    try:
        ret=random.gauss(args[0],args[1])
    except ValueError:
        return None

    return ret

gaussdistribution.registered=True


def sqroot(*args):

    """
    .. function:: sqroot(int) -> int

    Returns the square root of a given argument.

    Examples:

    >>> table1('''
    ... 25
    ... ''')
    >>> sql("select sqroot(a) from table1")
    sqroot(a)
    ---------
    5.0

    """

    try:
        ret=math.sqrt(args[0])
    except ValueError:
        return None
    
    return ret

sqroot.registered=True

def safediv(*args):

    """
    .. function:: safediv(int, int, int) -> int

    Returns the first argument, when the division of the two subsequent numbers
    includes zero in denominator (i.e. in third argument)

    Examples:

    >>> sql("select safeDiv(1,5,0)")
    safeDiv(1,5,0)
    --------------
    1

    """

    if args[2]==0:
        return args[0]
    else:
        return (args[1]/args[2])



safediv.registered=True

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
