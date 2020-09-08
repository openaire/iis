import Queue
import setpath
import functions
import datetime
from lib import iso8601

__docformat__ = 'reStructuredText en'

def timedelta2millisec(tdelta):
    return tdelta.days*24*60*60*1000+tdelta.seconds*1000+tdelta.microseconds/1000

class mindtdiff:
    """
    .. function:: mindtdiff(date)

    Returns the minimum difference *date* values of the group in milliseconds. Input dates should be in :ref:`ISO 8601 format <iso8601>`.

    Examples:

    >>> table1('''
    ... '2007-01-01 00:03:13'
    ... '2007-01-01 00:03:27'
    ... '2007-01-01 00:03:36'
    ... '2007-01-01 00:04:39'
    ... '2007-01-01 00:04:40'
    ... '2007-01-01 00:04:49'
    ... ''')
    >>> sql("select mindtdiff(a) from table1")
    mindtdiff(a)
    ------------
    1000

.. doctest::
    :hide:
    
    >>> sql("select mindtdiff(a) from (select '2005-01-01' as a) ")
    mindtdiff(a)
    ------------
    None
    >>> sql("select mindtdiff(a) from (select 5 as a where a!=5) ")
    mindtdiff(a)
    ------------
    None
    
    """
    registered=True

    def __init__(self):
        self.dates=Queue.PriorityQueue()

    def step(self, *args):
        if not args:
            raise functions.OperatorError("mindtdiff","No arguments")
        dt=iso8601.parse_date(args[0])
        self.dates.put_nowait(dt)



    def final(self):
        mindiff=None
        dtp=None
        if not self.dates:
            return
        while not self.dates.empty():
            if not mindiff:
                if not dtp:
                    dtp=self.dates.get_nowait()
                    continue
            dt=self.dates.get_nowait()
            diff=timedelta2millisec(dt-dtp)            
            if mindiff==None:
                mindiff=diff
            elif mindiff>diff:
                mindiff=diff
            dtp=dt
            import types
            
        return mindiff

class avgdtdiff:
    """
    .. function:: avgdtdiff(date)

    Returns the average difference *date* values of the group in milliseconds. Input dates should be in :ref:`ISO 8601 format <iso8601>`.

    Examples:
    
    >>> table1('''
    ... '2007-01-01 00:04:37'
    ... '2007-01-01 00:04:39'
    ... '2007-01-01 00:04:40'
    ... '2007-01-01 00:04:49'
    ... ''')
    >>> sql("select avgdtdiff(a) from table1")
    avgdtdiff(a)
    ------------
    3000.0


.. doctest::
    :hide:


    >>> sql("select avgdtdiff(a) from (select '2005-01-01' as a) ")
    avgdtdiff(a)
    ------------
    None
    >>> sql("select avgdtdiff(a) from (select 5 as a where a!=5) ")
    avgdtdiff(a)
    ------------
    None
    """
    registered=True

    def __init__(self):
        self.dates=Queue.PriorityQueue()

    def step(self, *args):
        if not args:
            raise functions.OperatorError("avgdtdiff","No arguments")
        dt=iso8601.parse_date(args[0])
        self.dates.put_nowait(dt)



    def final(self):
        avgdiff=0
        cntdiff=0
        dtp=None        
        while not self.dates.empty():
            if avgdiff==0:
                if not dtp:
                    cntdiff+=1
                    dtp=self.dates.get_nowait()
                    continue
            dt=self.dates.get_nowait()
            diff=timedelta2millisec(dt-dtp)
            cntdiff+=1
            avgdiff+=diff
            dtp=dt
        if cntdiff<2:
            return None
        return float(avgdiff)/cntdiff

class dategroupduration:
    """
    .. function:: dategroupduration(date)

    Returns the duration of the group of dates in seconds. Input dates should be in :ref:`ISO 8601 format <iso8601>`.

    Examples:

    >>> table1('''
    ... '2007-01-01 00:04:37'
    ... '2007-01-01 00:04:39'
    ... '2007-01-01 00:04:40'
    ... '2007-01-01 00:04:49'
    ... ''')
    >>> sql("select dategroupduration(a) from table1")
    dategroupduration(a)
    --------------------
    12

    >>> sql("select dategroupduration(a) from (select '2005-01-01' as a) ")
    dategroupduration(a)
    --------------------
    0

    """
    registered=True

    def __init__(self):
        self.datemin = None
        self.datemax = None

    def step(self, *args):
        pdate = iso8601.parse_date(args[0])

        if self.datemin == None:
            self.datemin = pdate

        if self.datemax == None:
            self.datemax = pdate

        if pdate < self.datemin:
            self.datemin = pdate

        if pdate > self.datemax:
            self.datemax = pdate

    def final(self):
        if self.datemin == None or self.datemax == None:
            return 0

        diff=self.datemax - self.datemin

        return diff.days*86400+diff.seconds


class frecencyindex:
    """
    .. function:: frecencyindex(date)

    Returns the frecency Index which is computed based on a set of *date* values, using predifend time-windows.
    Input dates should be in :ref:`ISO 8601 format <iso8601>`.

    Examples:

    >>> table1('''
    ... '2011-04-01 00:04:37'
    ... '2011-01-01 00:04:39'
    ... '2011-02-12 00:04:40'
    ... '2011-02-14 00:04:49'
    ... ''')
    >>> sql("select frecencyindex(a) from table1")
    frecencyindex(a)
    ----------------
    2.9

    """
    registered=True

    def __init__(self):
        self.monthCounter=0
        self.trimesterCounter=0
        self.semesterCounter=0
        self.yearCounter=0
        self.twoyearsCounter=0

    def step(self, *args):
        if not args:
            raise functions.OperatorError("frecencyindex","No arguments")

        now = datetime.datetime.now()
        now = iso8601.parse_date(now.strftime("%Y-%m-%d %H:%M:%S"))
        d = args[0].replace('T',' ')
        dt = iso8601.parse_date(args[0].replace('Z',''))
        diff=now-dt

        if (diff.days)<30:
                    self.monthCounter+=1
        elif (diff.days)<3*30:
                    self.trimesterCounter+=1
        elif (diff.days)<6*30:
                    self.semesterCounter+=1
        elif (diff.days)<12*30:
                    self.yearCounter+=1
        elif (diff.days)<24*30:
                    self.twoyearsCounter+=1



    def final(self):

        return self.monthCounter*1 + self.trimesterCounter*0.7 + self.semesterCounter*0.5 + self.yearCounter*0.3+ self.twoyearsCounter*0.2

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
        