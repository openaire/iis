from . import setpath
import functions
import random
# coding: utf-8
import math
import json
from fractions import Fraction

def randomrange(*args):

    """
    .. function:: randomrange(start, end, step) -> int

    Returns a random number in the defined range

    Examples:

    >>> sql("select randomrange(0, 68, 1)") # doctest: +ELLIPSIS
    randomrange(0, 68, 1)
    ---------------------
    ...

    >>> sql("select randomrange(0, 68)") # doctest: +ELLIPSIS
    randomrange(0, 68)
    ------------------
    ...

    """

    try:
        s = 1
        if len(args) >= 3:
            s = args[2]
        ret=random.randrange(args[0], args[1], s)
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

safediv.registered = True


def simplify_fraction(f):
    """
    .. function:: simplify_fraction(Fraction) -> int or float or Fraction

    Takes as input a Fraction and returns the equivalent int or float.
    In the case the int or float cannot be represented, the function returns the Fraction in json format

    Examples:
    >>> simplify_fraction(Fraction(50,1))
    50

    >>> simplify_fraction(Fraction(50,2))
    25

    >>> simplify_fraction(Fraction(55555555294967297,2))
    '[55555555294967297, 2]'
    """

    if f.denominator == 1 and f.numerator < 9223372036854775808:
        return f.numerator
    elif float(f) < 4294967296.0:
        return float(f)
    else:
        return json.dumps([f.numerator, f.denominator])


def farith(*args):
    """
    .. function:: farith(calc) -> float or Fraction

    Takes as input a mathematical expression in polish notation and computes the result using fractional computation

    Examples:

    >>> sql("select farith('+',5,7)" )
    farith('+',5,7)
    ---------------
    12

    >>> sql("select farith('-','*','/',15,'-',7,'+',1,1,3,'+',2,'+',1,1)" )
    farith('-','*','/',15,'-',7,'+',1,1,3,'+',2,'+',1,1)
    ----------------------------------------------------
    5
    """

    s = []
    for i in reversed(args):
        if i in ('*', '/', '-', '+'):
            operand1 = s.pop()
            operand2 = s.pop()
            if i == '+':
                operand = operand1 + operand2
            elif i == '-':
                operand = operand1 - operand2
            elif i == '/':
                operand = operand1 / operand2
            elif i == '*':
                operand = operand1 * operand2
            s.append(operand)
        else:
            if type(i) in (int, float, int):
                operand = Fraction(i)
                s.append(operand)
            else:
                try:
                    s.append(Fraction(*json.loads(i)))
                except ValueError as e:
                    raise functions.OperatorError('farith',"invalid expression found: '" + i +"'")

    return simplify_fraction(s.pop())

farith.registered = True


def tonumber(*args):

    """
    .. function:: tonumber(variable) -> int or float

    Convert variable, whose type is str or unicode, to int or float, if it is feasible

    Examples:

    >>> sql("select tonumber('12.3') as val")
    val
    ----
    12.3

    >>> sql("select tonumber(12.3) as val")
    val
    ----
    12.3

    >>> sql("select tonumber('not a number') as val")
    val
    ------------
    not a number

    >>> sql("select tonumber(null) as val")
    val
    ----
    None

    """

    if type(args[0]) not in (str, str):
        return args[0]

    try:
        ret = int(args[0])
    except ValueError:
        try:
            ret = float(args[0])
        except ValueError:
            return args[0]

    return ret

tonumber.registered = True

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
