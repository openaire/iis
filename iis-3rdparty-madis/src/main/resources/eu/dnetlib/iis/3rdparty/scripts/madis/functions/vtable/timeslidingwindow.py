"""

.. function:: timeslidingwindow(timewindow, timecolumn) -> query results

Returns the query input results annotated with the window id as an extra column.
The following arguments can be passed as parameters:

timewindow: It can be a numeric value that specifies the time length of
the window (in seconds). 

timecolumn: It is the index of the temporal column (starting from 0) For 
the moment, we assume that the data is ordered by the temporal column that
 the user gives as input in ascending order.  




    Examples::

    >>> table1('''
    ... "12.05.2010 00:00:00"
    ... "12.05.2010 00:01:00"
    ... "12.05.2010 00:02:00"
    ... "12.05.2010 00:03:00"
    ... "12.05.2010 00:04:00"
    ... ''')
    >>> sql("timeslidingwindow timewindow:180 timecolumn:0 select * from table1")
    wid | a
    -------------------------
    0   | 12.05.2010 00:00:00
    0   | 12.05.2010 00:01:00
    0   | 12.05.2010 00:02:00
    0   | 12.05.2010 00:03:00
    1   | 12.05.2010 00:01:00
    1   | 12.05.2010 00:02:00
    1   | 12.05.2010 00:03:00
    1   | 12.05.2010 00:04:00
    >>> table1('''
    ... "12.05.2010 00:00:00"
    ... "12.05.2010 00:01:00"
    ... "12.05.2010 00:01:00"
    ... "12.05.2010 00:02:00"
    ... "12.05.2010 00:03:00"
    ... "12.05.2010 00:04:00"
    ... "12.05.2010 00:05:00"
    ... ''')

    ... ''')
    >>> sql("timeslidingwindow timewindow:120 timecolumn:0 select * from table1")
    wid | a
    -------------------------
    0   | 12.05.2010 00:00:00
    0   | 12.05.2010 00:01:00
    0   | 12.05.2010 00:01:00
    0   | 12.05.2010 00:02:00
    1   | 12.05.2010 00:01:00
    1   | 12.05.2010 00:01:00
    1   | 12.05.2010 00:02:00
    1   | 12.05.2010 00:03:00
    2   | 12.05.2010 00:02:00
    2   | 12.05.2010 00:03:00
    2   | 12.05.2010 00:04:00
    3   | 12.05.2010 00:03:00
    3   | 12.05.2010 00:04:00
    3   | 12.05.2010 00:05:00

    >>> table2('''
    ... "12/05/2010 00:00:00"
    ... "12/05/2010 00:01:00"
    ... "12/05/2010 00:02:00"
    ... ''')


    ... ''')
    >>> sql("timeslidingwindow timewindow:180 timecolumn:0  select * from table2")
    wid | a
    -------------------------
    0   | 12/05/2010 00:00:00
    0   | 12/05/2010 00:01:00
    0   | 12/05/2010 00:02:00

"""

import setpath
import vtbase
import functions
from collections import deque
import time
from lib.dateutil import parser

### Classic stream iterator
registered = True


class TimeSlidingWindow(vtbase.VT):
    def VTiter(self, *parsedArgs, **envars):
        largs, dictargs = self.full_parse(parsedArgs)

        if 'query' not in dictargs:
            raise functions.OperatorError(__name__.rsplit('.')[-1], "No query argument ")
        query = dictargs['query']

        if 'timewindow' not in dictargs:
            raise functions.OperatorError(__name__.rsplit('.')[-1], "No TimeWindow argument ")
        else:
            winlen = int(dictargs['timewindow'])

        if 'timecolumn' not in dictargs:
            raise functions.OperatorError(__name__.rsplit('.')[-1], "No timecolumn argument ")
        else:
            timecolumn = int(dictargs['timecolumn'])

        cur = envars['db'].cursor()

        c = cur.execute(query, parse=False)

        try:
            yield [('wid', 'integer')] + list(cur.getdescriptionsafe())
        except StopIteration:
            try:
                raise
            finally:
                try:
                    c.close()
                except:
                    pass

        wid = 0
        secs = 0
        row = c.next()
        firstTime = int(time.mktime(parser.parse(row[timecolumn], fuzzy=True).timetuple()))

        head = {firstTime: [row]}
        window = deque([])
        while row:
            prev = row

            try:
                row = c.next()
            except StopIteration:
                if wid == 0:
                    for k in head.keys():
                        for t in head[k]:
                            yield (wid,) + t
                    for rl in window:
                        for k in rl.keys():
                            for t in rl[k]:
                                yield (wid,) + t
                break

            secs = int(time.mktime(parser.parse(row[timecolumn], fuzzy=True).timetuple()))

            if secs <= firstTime + winlen:
                if prev[0] == row[timecolumn] and window:
                    old = window.pop()[secs]
                    old.append(row)
                    rowlist = {secs: old}
                else:
                    rowlist = {secs: [row]}

                window.append(rowlist)
            else:
                if wid == 0:
                    for k in head.keys():
                        for t in head[k]:
                            yield (wid,) + t
                    for rl in window:
                        for k in rl.keys():
                            for t in rl[k]:
                                yield (wid,) + t
                while secs > firstTime + winlen and window:
                    try:
                        head = window.popleft()
                        firstTime = head.keys()[0]
                    except IndexError:
                        break

                rowlist = {secs: [row]}
                window.append(rowlist)
                wid += 1
                for k in head.keys():
                    for t in head[k]:
                        yield (wid,) + t
                for rl in window:
                    for k in rl.keys():
                        for t in rl[k]:
                            yield (wid,) + t




def Source():
    return vtbase.VTGenerator(TimeSlidingWindow)


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
