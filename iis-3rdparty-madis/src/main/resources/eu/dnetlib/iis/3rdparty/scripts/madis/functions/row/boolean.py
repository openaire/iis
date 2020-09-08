# coding: utf-8

import setpath
import functions

def ifthenelse(*args):
    """
    .. function:: ifthenelse(condition, x, y)
    
        Returns *x* if *condition* is true, else returns *y*.

    .. templateforparams Parameters:
        :condition: exception type
        :x: exception value
        :y: traceback object
        :returns: true or false

    .. note::

        The difference with the *if* construct in most programming languages
        is that *x* and *y* expressions will always be evaluated.

    Examples:

    >>> sql("select ifthenelse(1>0,'yes','no') as answer")
    answer
    ------
    yes
    """
    if len(args)<2:
        raise functions.OperatorError("ifthenelse","operator needs at least two inputs")

    if args[0]:
        return args[1]
    else:
        if len(args)>2:
            return args[2]
        return None

ifthenelse.registered=True

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
