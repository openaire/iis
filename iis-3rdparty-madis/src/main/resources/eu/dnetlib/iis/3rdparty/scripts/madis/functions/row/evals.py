# coding: utf-8

import setpath
import datetime

import functions
from string import Template

class myTemplate(Template):
    delimiter=r'%'
    pattern = r"""
    %(delim)s(?:
      (?P<escaped>%(delim)s) |   # Escape sequence of two delimiters
      (?P<named>^)      |   # delimiter and a Python identifier
      {(?P<braced>%(id)s)}   |   # delimiter and a braced identifier
      (?P<invalid>)              # Other ill-formed delimiter exprs
    )
    """%{'delim' : delimiter, 'id': Template.idpattern }


def pyeval(*args):

    """
    .. function:: pyeval(expression)

    Evaluates with Python the expression/s given and returns the result

    >>> sql("pyeval '1+1'")
    pyeval('1+1')
    -------------
    2
    >>> sql("select var('test')")  # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ...
    OperatorError: Madis SQLError:
    Operator VAR: Variable 'test' does not exist
    >>> sql("select var('test', pyeval('1+1'))")
    var('test', pyeval('1+1'))
    --------------------------
    2
    >>> sql("select var('test')")
    var('test')
    -----------
    2
    >>> sql('''pyeval '1+1' '"-"' '3+1' ''')
    pyeval('1+1','"-"','3+1')
    -------------------------
    2-4
    >>> sql("var 'testvar' of select 5")
    var('testvar',(select 5))
    -------------------------
    5
    >>> sql("pyeval 'testvar+5'")
    pyeval('testvar+5')
    -------------------
    10
    >>> sql('''pyeval keywords('lala') ''')
    pyeval('keywords(''lala'')')
    ----------------------------
    lala
    """

    if len(args)==0:
        return
    
    r=''
    for i in args:
        r=r+str(eval(i, functions.variables.__dict__, functions.rowfuncs.__dict__))

    return r

pyeval.registered=True

def pyfun(*args):
    """
    .. function:: pyfun(pyfunction, parameters)

    Calls a python function and returns the result. If an error occurs, it throws an
    exception.

    >>> sql("select pyfun('math.sqrt', 25)")
    pyfun('math.sqrt', 25)
    ----------------------
    5.0
    >>> sql("select pyfun('math.log10', 100)")
    pyfun('math.log10', 100)
    ------------------------
    2.0
    >>> sql("select pyfun('math.log10', -1)") # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ...
    OperatorError: Madis SQLError:
    Operator PYFUN: math.log10: math domain error
    """

    if len(args)==0:
        return

    fsplit=args[0].split('.')
    try:
        f=__import__(fsplit[0])
        for i in fsplit[1:]:
            f=f.__dict__[i]
    except KeyboardInterrupt:
        raise
    except:
        try:
            f=__import__('libexternal'+'.'+fsplit[0])
            for i in fsplit:
                f=f.__dict__[i]
        except:
            raise functions.OperatorError("pyfun","didn't find function: "+args[0])

    try:
        res=f(*args[1:])
    except Exception, e:
        raise functions.OperatorError("pyfun",args[0]+": "+functions.mstr(e))

    if res is None or type(res) in (int,float, str, unicode):
        return res
    else:    
        return repr(f(*args[1:]))

pyfun.registered=True

def pyfunerrtonul(*args):
    """
    .. function:: pyfunerrtonul(pyfunction, parameters)

    Calls a python function and returns the result. If an error occurs it returns
    *null*.

    >>> sql("select pyfunerrtonul('math.sqrt', -1)")
    pyfunerrtonul('math.sqrt', -1)
    ------------------------------
    None
    >>> sql("select pyfunerrtonul('math.log10', -1)")
    pyfunerrtonul('math.log10', -1)
    -------------------------------
    None
    """

    if len(args)==0:
        return

    fsplit=args[0].split('.')
    try:
        f=__import__(fsplit[0])
        for i in fsplit[1:]:
            f=f.__dict__[i]
    except:
        try:
            f=__import__('libexternal'+'.'+fsplit[0])
            for i in fsplit:
                f=f.__dict__[i]
        except:
            raise functions.OperatorError("pyfunerrtonul","didn't find function: "+args[0])

    try:
        res=f(*args[1:])
    except Exception, e:
        return None

    if res is None or type(res) in (int,float, str, unicode):
        return res
    else:
        return repr(f(*args[1:]))

pyfunerrtonul.registered=True

def subst(*args):
    """
    .. function:: subst(str, variables)

    Substitutes the special text markers with the variables values.

    >>> sql('''subst 'Variable %s has value %s' 'var1' '5' ''')
    subst('Variable %s has value %s','var1','5')
    --------------------------------------------
    Variable var1 has value 5
    
    >>> sql('''select subst('Variable %s has value %d','var2',5) ''')
    subst('Variable %s has value %d','var2',5)
    ------------------------------------------
    Variable var2 has value 5

    >>> sql('''var 'testvar' 'testvalue' ''')
    var('testvar','testvalue')
    --------------------------
    testvalue

    >>> sql('''select subst('Variable %{testvar}1 %{testvar1} has value %s', 5) ''')
    subst('Variable testvalue1 %{testvar1} has value %s', 5)
    --------------------------------------------------------
    Variable testvalue1 %{testvar1} has value %s

    """
    if len(args)==0:
        return

    str=myTemplate(args[0]).safe_substitute(functions.variables.__dict__)
    
    if len(args)==1:
        return str

    try:
        str=str%args[1:]
    except:
        pass

    return str

subst.registered=True

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
