"""
.. function:: oaiget(url, verb, metadataPrefix,...)

Fetches data from an OAIPMH service, using resumption tokens to fetch large datasets.

- If no *verb* is provided then *verb* is assumed to be 'ListRecords'.
- If no *metadataPrefix* is provided then *verb* is assumed to be 'ListMetadataFormats', which will list
  all metadata formats.

:Returned table schema:
    Column C1 as text

Examples:

    >>> sql("select * from oaiget('verb:ListRecords', 'metadataPrefix:ctxo')")    # doctest:+ELLIPSIS +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ...
    OperatorError: Madis SQLError:
    Operator OAIGET: An OAIPMH URL should be provided

    >>> sql("select * from (oaiget verb:ListRecords metadataPrefix:ctxo 'http://oaiurl' )")    # doctest:+ELLIPSIS +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ...
    OperatorError: Madis SQLError:
    Operator OAIGET: <urlopen error [Errno -2] Name or service not known>

"""
import vtbase
import functions
import time

registered=True
external_stream=True

class oaiget(vtbase.VT):
    def VTiter(self, *parsedArgs, **envars):

        import urllib2
        import re

        def buildURL(baseurl, opts):
            return '?'.join([ baseurl, '&'.join([x+'='+unicode(y) for x,y in opts if y!=None]) ])

        def buildopener():
            o = urllib2.build_opener()
            o.addheaders = [
             ('Accept', '*/*'),
             ('Connection', 'Keep-Alive'),
             ('Content-type', 'text/xml')
            ]
            return o

        yield [('c1', 'text')]

        opts= self.full_parse(parsedArgs)[1]

        if 'verb' not in opts:
            opts['verb']='ListRecords'
        if 'metadataPrefix' not in opts:
            opts['verb']='ListMetadataFormats'
        if 'resumptionToken' in opts:
            opts['verb']='ListRecords'
        if 'http' not in opts:
            raise functions.OperatorError(__name__.rsplit('.')[-1],"An OAIPMH URL should be provided")

        baseurl='http:'+opts['http']

        del(opts['http'])

        opts=list(opts.iteritems())

        findrestoken=re.compile(r""">([^\s]+?)</resumptionToken>""", re.DOTALL| re.UNICODE)

        resumptionToken=lastResToken=None
        firsttime=True
        url=buildURL(baseurl, opts+[('resumptionToken', resumptionToken)])

        opener=buildopener()

        errorcount=0
        while True:
            try:
                for i in opener.open(url, timeout=1200):
                    if resumptionToken==None:
                        t=findrestoken.search(i)
                        if t:
                            errorcount=0
                            resumptionToken=t.groups()[0]
                    yield (unicode(i.rstrip("\n"), 'utf-8'),)
                if resumptionToken==None:
                    break
                url=buildURL(baseurl, [(x,y) for x,y in opts if x=='verb']+[('resumptionToken', resumptionToken)])
                lastResToken=resumptionToken
                resumptionToken=None
                firsttime=False
            except KeyboardInterrupt:
                raise           
            except Exception,e:
                if errorcount<10 and not firsttime:
                    time.sleep(2**errorcount)
                    errorcount+=1
                    opener=buildopener()
                else:
                    if lastResToken==None:
                        raise functions.OperatorError(__name__.rsplit('.')[-1], e)
                    else:
                        raise functions.OperatorError(__name__.rsplit('.')[-1], str(e)+'\n'+'To continue use:\nresumptionToken:'+str(lastResToken))


def Source():
    return vtbase.VTGenerator(oaiget)


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
