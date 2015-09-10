# coding: utf-8

import setpath
import functions
import os

def var(*args):

    """
    .. function:: var(varname[, value]) -> value

    Sets (if both varname and value are given) or returns (if only varname is given) the contents of a variable.

    Examples:

    >>> sql("var 'v'")  # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
        ...
    OperatorError: Madis SQLError:
    Operator VAR: Variable 'v' does not exist
    >>> sql("var 'v' 5")
    var('v','5')
    ------------
    5
    >>> sql("var 'v'")
    var('v')
    --------
    5
    >>> sql("select var('v')")
    var('v')
    --------
    5
    """

    if len(args)==0:
        return str(functions.variables.__dict__)

    var=args[0]

    if len(args)==1:
        if hasattr(functions.variables,var):
            return functions.variables.__dict__[var]
        else:
            raise functions.OperatorError('var', "Variable '" +var+"' does not exist")
    elif len(args)==2:
        functions.variables.__dict__[var]=args[1]
        return functions.variables.__dict__[var]
    else:
        return None

var.registered=True

def getvar(*args):

    """
    .. function:: getvar(varname) -> value

    Returns the value of varname. This function exists so as to be able to force
    SQLite's engine to always evaluate the *getvar* function. This can be achieved
    by givine to the function an always changing second parameter.

    Examples:

    >>> sql("var 't' 5")
    var('t','5')
    ------------
    5
    >>> sql("getvar 't'")
    getvar('t')
    -----------
    5
    >>> sql("select getvar('t', random())")
    getvar('t', random())
    ---------------------
    5
    """

    try:
        return functions.variables.__dict__[args[0]]
    except KeyError:
        raise functions.OperatorError('var', "Variable '" +var+"' does not exist")

getvar.registered=True

def requirevars(*args):
    """
    .. function:: requirevars(varname1, [varname2,...])

    Checks if all variables (varname1,...) exist. If not it throws an exception.

    Examples:

    >>> sql("var 'cv1' 5")
    var('cv1','5')
    --------------
    5
    >>> sql("var 'cv2' 10")
    var('cv2','10')
    ---------------
    10
    >>> sql("requirevars 'cv1' 'cv2'")
    requirevars('cv1','cv2')
    ------------------------
    1
    >>> sql("requirevars cv1 cv2")
    requirevars('cv1 cv2')
    ----------------------
    1
    >>> sql("requirevars 'cv1' 'testvar'") # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ...
    OperatorError: Madis SQLError:
    Operator REQUIREVARS: Variable testvar isn't initialized
    """

    for v in (' '.join(args).strip()).split():
        if not hasattr(functions.variables, v):
            raise functions.OperatorError("requirevars","Variable %s isn't initialized"%v)
    return 1

requirevars.registered=True

def flowname(*args):
    """
    .. function:: flowname([str])

    Sets and retrieves, 'flowname' variable

    Examples:

    >>> sql("flowname test flow ")
    flowname('test flow')
    ---------------------
    test flow
    >>> sql("flowname")
    flowname()
    ----------
    test flow
    >>> sql("flowname 'arg1' arg2") # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
        ...
    OperatorError: Madis SQLError:
    Operator FLOWNAME: Flowname accepts only 1 argument
    """

    var='flowname'
    if len(args)>1:
        raise functions.OperatorError('flowname','Flowname accepts only 1 argument')

    if len(args)==0 and hasattr(functions.variables,var):
        return str(functions.variables.__dict__[var])
    elif len(args)==1:
        functions.variables.__dict__[var]=' '.join( [str(x) for x in args[0:] ] )
        return str(functions.variables.__dict__[var])
    else:
        return None
flowname.registered=True

def setexecdb(*args):

    """
    .. function:: setexecdb(str)

    Sets the database path/filename for exec operator.

    """

    var='execdb'
    if len(args)==0 and hasattr(functions.variables,var):
        return str(functions.variables.__dict__[var])
    else:
        functions.variables.__dict__[var]=str(os.path.abspath(os.path.expandvars(os.path.expanduser(os.path.normcase(args[0])))))
        return str(functions.variables.__dict__[var])

setexecdb.registered=True

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
