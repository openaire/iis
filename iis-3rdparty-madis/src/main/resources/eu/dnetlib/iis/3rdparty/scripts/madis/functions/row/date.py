# coding: utf-8
import functions
import datetime
from lib import iso8601
from lib.dateutil import parser

def cleantimezone(*args):

    """
    .. function:: cleantimezone(date) -> date

    Specialized function that removes timezone information from date string

    Examples:

    >>> table1('''
    ... '2009-01-01T01:03:13+0100'
    ... '2009-01-01T01:03:13-0100'
    ... '2009-01-01T01:03:13+01:00'
    ... '2009-01-01T01:03:13-01:00'
    ... '2009-01-01T01:03:13+01'
    ... '2009-01-01T01:03:13-01'
    ... ''')
    >>> sql("select cleantimezone(a) from table1")
    cleantimezone(a)
    -------------------
    2009-01-01 01:03:13
    2009-01-01 01:03:13
    2009-01-01 01:03:13
    2009-01-01 01:03:13
    2009-01-01 01:03:13
    2009-01-01 01:03:13
    """

    d = args[0].replace('T',' ')
    tindex = d.find('+')
    mindex = d.rfind('-')
    if tindex<>-1:
        return d[0:tindex]
    elif mindex <>-1 and mindex>13:
        return d[0:mindex]
    else:
        return d;

cleantimezone.registered=True


def activityindex(*args):

    """
    .. function:: activityIndex(date, c1, c2) -> int

    Specialized function that classifies the provided date argument into a 6-point scale (0 to 5)

    Examples:

    >>> table1('''
    ... '2009-01-01T01:32:03Z'
    ... '2010-01-01T00:03:13Z'
    ... '2010-12-31T00:03:13Z'
    ... '2011-04-01T00:03:13Z'
    ... ''')
    >>> sql("select activityIndex(a) from table1")
    activityIndex(a)
    ----------------
    0
    1
    3
    5
    """
    now = datetime.datetime.now()
    now = iso8601.parse_date(now.strftime("%Y-%m-%d %H:%M:%S"))
    d = args[0].replace('T',' ')
    dt = iso8601.parse_date(args[0].replace('Z',''))  
    diff=now-dt

    if (diff.days)<30:
                    return 5
    elif (diff.days)<3*30:
                    return 4
    elif (diff.days)<6*30:
                    return 3
    elif (diff.days)<12*30:
                    return 2
    elif (diff.days)<24*30:
                    return 1
    elif (diff.days)>=24*30:
                    return 0
    else:
        return -1;

activityindex.registered=True

def sectohuman(*args):

    """
    .. function:: sectohuman(sec) -> human readable format

    Converts a number of seconds to human readable format.

    Examples:

    >>> table1('''
    ... 3
    ... 63
    ... 10000
    ... 100000
    ... 1000000
    ... ''')
    >>> sql("select sectohuman(a) from table1")
    sectohuman(a)
    ------------------------------
    3 sec
    1 min 3 sec
    2 hours 46 min 40 sec
    1 day 3 hours 46 min 40 sec
    11 days 13 hours 46 min 40 sec
    """

    secs=int(args[0])
    h=''
    days=secs/86400
    if days > 0:
        h+=str(days)+' day'
        if days > 1:
            h+='s'
        h+=' '
        secs=secs % 86400
    hours=secs/3600
    if hours > 0:
        h+=str(hours)+' hour'
        if hours > 1:
            h+='s'
        h+=' '
        secs=secs % 3600
    mins=secs/60
    if mins > 0:
        h+=str(mins)+' min '
        secs=secs % 60
    if secs > 0:
        h+=str(secs)+' sec'

    return h

sectohuman.registered=True

def datestrf2isoweek(*args):

    """
    .. function:: dateisoweek2week52(sec) -> isoweek

    Converts an ISOweek (having weeks in range [0,53]) to an ISOweek
    format which has weeks in range [1,53]. This function is usefull for
    producing week statistics which do not have incomplete weeks.

    Examples:

    >>> table1('''
    ... 2007-12-31
    ... 2010-01-01
    ... ''')

    >>> sql("select strftime('%YW%W',a) from table1")
    strftime('%YW%W',a)
    -------------------
    2007W53
    2010W00

    >>> sql("select datestrf2isoweek(strftime('%YW%W',a)) from table1")
    datestrf2isoweek(strftime('%YW%W',a))
    -------------------------------------
    2007W53
    2009W53
    """

    year=int(args[0][0:4])
    week=args[0][-2:]

    if week=='00':
        year-=1
        week='53'

    return str(year)+'W'+week

datestrf2isoweek.registered=True


def date2iso(*args):

    """
    .. function:: date2iso(sec) -> ISO Datetime

    Converts an input date to ISO-8601 date format. It tries to autodetect, the
    input date format.

    Examples:

    >>> table1('''
    ... 2007-12-31
    ... 2010-01-01
    ... 2010W06
    ... "18/Jan/2011:11:13:00 +0100"
    ... ''')

    >>> sql("select date2iso(a) from table1")
    date2iso(a)
    -------------------------
    2007-12-31T00:00:00+00:00
    2010-01-01T00:00:00+00:00
    2010-02-05T00:00:00+00:00
    2011-01-18T11:13:00+01:00

    """

    date = args[0]
    try:
        date = iso8601.parse_date(date)
    except iso8601.ParseError:
        date = parser.parse(date, fuzzy=True)

    return date.isoformat()

date2iso.registered=True


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
