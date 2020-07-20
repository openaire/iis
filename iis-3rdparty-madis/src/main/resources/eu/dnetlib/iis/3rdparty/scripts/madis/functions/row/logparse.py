# coding: utf-8
import re
import functions

apache_log_split=re.compile('^(\\S*) (\\S*) (\\S*) (\\[[^\\]]+\\]) \\"(\\w+) ([^"\\\\]*(?:\\\\.[^"\\\\]*)*) HTTP/([\\d.]+)\\" (\\S*) (\\S*) \\"([^"\\\\]*(?:\\\\.[^"\\\\]*)*)\\" \\"([^\\"]*)\\"$', re.UNICODE)

months = {
    'Jan':'01',
    'Feb':'02',
    'Mar':'03',
    'Apr':'04',
    'May':'05',
    'Jun':'06',
    'Jul':'07',
    'Aug':'08',
    'Sep':'09',
    'Oct':'10',
    'Nov':'11',
    'Dec':'12'
    }

def apachelogsplit(*args):

    """
    .. function:: apachelogsplit(apache_log_line) -> [ip, ident, authuser, date, request, status, bytes, referrer, useragent]

    Breaks a single apache log row into multiple fields.

    Examples:

    >>> table1('''
    ... '1.1.1.1 - - [01/Feb/2001:01:02:03 +0001] "HEAD /test.com HTTP/1.1" 200 - "-" "reftest"'
    ... ''')
    >>> sql("select apachelogsplit(a) from table1")
    ip      | ident | authuser | date                     | method | uri       | httpver | status | bytes | referrer | useragent
    ----------------------------------------------------------------------------------------------------------------------------
    1.1.1.1 | None  | None     | 2001-02-01T01:02:03+0001 | HEAD   | /test.com | 1.1     | 200    | None  | None     | reftest

    """

    yield ('ip', 'ident', 'authuser', 'date', 'method', 'uri', 'httpver', 'status', 'bytes', 'referrer', 'useragent')

    f=apache_log_split.match(''.join(args).strip())

    if f == None:
        raise functions.OperatorError("APACHELOGSPLIT", "Row function didn't receive any input")
    f=f.groups()

    f=[None if x=='-' else x for x in f]

    #parse date
    if f[3]!=None:
        if f[3][4:7] in months:
            f[3]=f[3][1:-1]
            date=f[3]
            f[3]=date[7:11]+'-'+months[date[3:6]]+'-'+date[0:2]+'T'+date[12:14]+':'+date[15:17]+':'+date[18:20]+date[21:]

    if f[7]!=None:
        f[7]=int(f[7])
    if f[8]!=None:
        f[8]=int(f[8])

    yield f

apachelogsplit.registered=True

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
