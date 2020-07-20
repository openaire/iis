"""
.. function:: diavgeiaget(url, verb, metadataPrefix,...)

    Fetches data from an OAIPMH service, using resumption tokens to fetch large datasets.

    - If no *verb* is provided then *verb* is assumed to be 'ListRecords'.
    - If no *metadataPrefix* is provided then *verb* is assumed to be 'ListMetadataFormats', which will list
      all metadata formats.

:Returned table schema:
    Column C1 as text

Examples:

    >>> sql("select * from diavgeiaget('verb:ListRecords', 'metadataPrefix:ctxo')")    # doctest:+ELLIPSIS +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ...
    OperatorError: Madis SQLError:
    Operator OAIGET: An OAIPMH URL should be provided

    >>> sql("select * from (diavgeiaget verb:ListRecords metadataPrefix:ctxo 'http://oaiurl' )")    # doctest:+ELLIPSIS +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ...
    OperatorError: Madis SQLError:
    Operator OAIGET: <urlopen error [Errno -2] Name or service not known>

"""
from functions.vtable import vtbase
import functions
import time

registered=True
external_stream=True

class diavgeiaget(vtbase.VT):
    def VTiter(self, *parsedArgs, **envars):
        
        def buildURL(baseurl, opts):
            return '?'.join([ baseurl, '&'.join([x+'='+unicode(y) for x,y in opts if y!=None]) ])

        import urllib2
        import re

        opts= self.full_parse(parsedArgs)[1]

        yield ('c1', 'text')

        if 'datefrom' not in opts:
            opts['datefrom']='01-01-1000'
        if 'output' not in opts:
            opts['output']='full'
        if 'order' not in opts:
            opts['order']='asc'
        if 'http' not in opts:
            opts['http']='//opendata.diavgeia.gov.gr/api/decisions'

        baseurl='http:'+opts['http']

        findcount=re.compile(r"""<count>[^\d]*?(\d+)[^\d]*?</count>""", re.DOTALL| re.UNICODE)
        findtotal=re.compile(r"""<total>[^\d]*?(\d+)[^\d]*?</total>""", re.DOTALL| re.UNICODE)
        findfrom=re.compile(r"""<from>[^\d]*?(\d+)[^\d]*?</from>""", re.DOTALL| re.UNICODE)

        count=total=fromv=lastfromv=None
        firsttime=True

        del(opts['http'])
        opts=list(opts.iteritems())
        url=buildURL(baseurl, opts)

        def buildopener():
            o = urllib2.build_opener()
            o.addheaders = [
             ('Accept', '*/*'),
             ('Connection', 'Keep-Alive'),
             ('Content-type', 'text/xml')
            ]
            return o

        opener=buildopener()

        errorcount=0
        while True:
            try:
                for i in opener.open( url, timeout=1200 ):
                    if count==None:
                        t=findcount.search(i)
                        if t:
                            count=int(t.groups()[0])
                    if total==None:
                        t=findtotal.search(i)
                        if t:
                            errorcount=0
                            total=int(t.groups()[0])
                    if fromv==None:
                        t=findfrom.search(i)
                        if t:
                            errorcount=0
                            fromv=int(t.groups()[0])
                    yield (unicode(i.rstrip("\n"), 'utf-8'),)
                if count==None or total==None or fromv==None:
                    break
                fromv=fromv+count
                if fromv>total:
                    break
                url=buildURL(baseurl, opts+[('from', fromv)])
                lastfromv=fromv
                count=total=fromv=None
                firsttime=False
            except Exception,e:
                if errorcount<10 and not firsttime:
                    time.sleep(2**errorcount)
                    errorcount+=1
                else:
                    if lastfromv==None:
                        raise functions.OperatorError(__name__.rsplit('.')[-1], e)
                    else:
                        raise functions.OperatorError(__name__.rsplit('.')[-1], str(e)+'\n'+'To continue, use the following "from" parameter:\n'+str(lastfromv))

def Source():
    return vtbase.VTGenerator(diavgeiaget)


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

