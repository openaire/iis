from datetime import datetime, timedelta
import setpath
from lib import iso8601


def tzconverter(*args):

    """
    .. function:: tzconverter(timestamp,offset)

    Returns timestamps converted from UTC to target timezone, indicated by the offset parameter.


    Example::

    >>> table1('''
    ... "2010-12-05T00:00:00+00:00"
    ... "2010-12-05T00:01:00+00:00"
    ... "2010-12-05T00:02:00+00:00"
    ... ''')

    ... ''')
    >>> sql("select a, tzconverter(a,'-01:00')  from table1 ")
    a                         | tzconverter(a,'-01:00')
    -----------------------------------------------------
    2010-12-05T00:00:00+00:00 | 2010-12-04T23:00:00-01:00
    2010-12-05T00:01:00+00:00 | 2010-12-04T23:01:00-01:00
    2010-12-05T00:02:00+00:00 | 2010-12-04T23:02:00-01:00

    ... ''')
    >>> sql("select a, tzconverter(a,'-01')  from table1 ")
    a                         | tzconverter(a,'-01')
    --------------------------------------------------
    2010-12-05T00:00:00+00:00 | 2010-12-04T23:00:00-01
    2010-12-05T00:01:00+00:00 | 2010-12-04T23:01:00-01
    2010-12-05T00:02:00+00:00 | 2010-12-04T23:02:00-01

    >>> sql("select a, tzconverter(a,'-0100')  from table1 ")
    a                         | tzconverter(a,'-0100')
    ----------------------------------------------------
    2010-12-05T00:00:00+00:00 | 2010-12-04T23:00:00-0100
    2010-12-05T00:01:00+00:00 | 2010-12-04T23:01:00-0100
    2010-12-05T00:02:00+00:00 | 2010-12-04T23:02:00-0100

    >>> sql("select a, tzconverter(a,'+00:30')  from table1 ")
    a                         | tzconverter(a,'+00:30')
    -----------------------------------------------------
    2010-12-05T00:00:00+00:00 | 2010-12-05T00:30:00+00:30
    2010-12-05T00:01:00+00:00 | 2010-12-05T00:31:00+00:30
    2010-12-05T00:02:00+00:00 | 2010-12-05T00:32:00+00:30


    """

    date = iso8601.parse_date(args[0])
    mins = 0
    sign = ''
    result = ''
    c = 0

    for i in args[1]:
        if c == 0:
            sign = args[1][0]
        elif c == 1:
            mins += int(args[1][1])*600
        elif c == 2:
            mins += int (args[1][2])*60
        elif c == 3 and args[1][3] == ':': #in this case i know what's next
            mins += int(args[1][4])*10 + int(args[1][5])
            break;
        elif c == 3:
            mins += int(args[1][3])*10
        elif c == 4:
            mins += int (args[1][4])
        c+=1

    if sign == '+':
        result = date + timedelta(minutes = mins)

    elif sign == '-':
        result = date - timedelta(minutes = mins)

    result =  str(result).replace(" ","T").replace("+00:00", args[1])

    return result

tzconverter.registered = True



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
