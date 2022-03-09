# coding: utf-8
import os

def fileextension(*args):

    """
    .. function:: fileextension(text) -> text

    Returns the extension of a given text argument.

    Examples:

    >>> table1('''
    ... "http://www.test.com/lalala.gif"
    ... "http://www.test.com/lalala.GIF"
    ... ''')
    >>> sql("select fileextension(a) from table1")
    fileextension(a)
    ----------------
    .gif
    .gif

    """

    try:
        ret=os.path.splitext(args[0])
    except ValueError:
        return None

    return ret[1].lower()

fileextension.registered = True


def filetext(*args):
    """
    .. function:: filetext(filename) -> text

    Returns the contents of the file in a single value

    Examples:

    >>> sql("select filetext('testing/sales.tsv')") #doctest:+ELLIPSIS +NORMALIZE_WHITESPACE
    filetext('testing/sales.tsv')
    ----------------------------------
    Cars        2010-01 Athens  200
    Cars        2010-02 Athens  130
    Bikes       2010-01 NY      10
    Bikes       2010-02 NY      30
    Cars        2010-01 NY      100
    Cars        2010-02 NY      160
    Cars        2010-01 Paris   70
    Cars        2010-02 Paris   20
    Bikes       2010-01 Paris   100
    Bikes       2010-02 Paris   20
    Boats       2010-01 Paris   200
    """

    try:
        with open(args[0], "rU") as f:
            data = f.read()
    except ValueError:
        return None
    return data

filetext.registered = True


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
