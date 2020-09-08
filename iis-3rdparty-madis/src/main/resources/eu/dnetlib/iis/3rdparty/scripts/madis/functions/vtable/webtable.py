"""
.. function:: webtable(url[,tableNumber])

    Returns the result of the first or the *tableNumber* HTML table from the *url*.

:Returned table schema:
    Column names same as HTML table column headers. If there are no headers columns are named as C1,C2....Cn

Examples:
    
    >>> sql("select * from webtable('http://en.wikipedia.org/wiki/List_of_countries_by_public_debt',2) order by 2 desc limit 3")
    Country | Public debt as % of GDP(CIA)[1] | Date1     | Gross government debt as % of GDP(IMF)[2] | Date2     | Region
    -----------------------------------------------------------------------------------------------------------------------------
    Belize  | 90.8                            | 2012 est. | 81.003                                    | 2012 est. | North America
    Sudan   | 89.3                            | 2012 est. | 112.15                                    | 2012 est. | Africa
    France  | 89.1                            | 2012 est. | 89.97                                     | 2012 est. | Europe
    
"""
import setpath
import functions
import urllib2
import vtbase
from lib import TableHTMLParser

registered=True
external_stream=True

class WebTable(vtbase.VT):
    def parse(self,*args):
        tableNum=1
        argsnum=len(args)
        if argsnum<1 or argsnum>2:
            raise functions.OperatorError(__name__.rsplit('.')[-1],"Wrong number of arguments")
        tableUrl=args[0]
        if argsnum==2:
            try:
                tableNum=int(args[1])
            except Exception:
                raise functions.OperatorError(__name__.rsplit('.')[-1],"Table number must be integer")
        return (tableUrl, tableNum)

    def VTiter(self,tableUrl, tableNum,**envars):
        tableiter = TableParse(tableUrl, tableNum)

        samplerow = tableiter.next()

        if type(samplerow) == tuple:
            yield [(header,'text') for header in samplerow]
        else:
            yield [('C'+str(i),'text') for i in range(1, len(samplerow)+1)]
            yield samplerow

        for r in tableiter:
            yield r


class TableParse:
    def __init__(self,tableUrl, tableNum):
        url = tableUrl

        try:
            txdata = None
            txheaders = {
                'User-Agent': 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)',
            }
            req = urllib2.Request(url, txdata, txheaders)
            self.ufile = urllib2.urlopen(req)
            headers = self.ufile.info()
        except Exception:
            raise functions.OperatorError(__name__.rsplit('.')[-1],"Cannot load url:'%s'" %(repr(url)))
        parser = TableHTMLParser.TableHTMLParser(tableNum)
        
        self.iterator=linkiter(self.ufile,parser.parse)
    def __iter__(self):
        return self
    def next(self):
        try:
            current = self.iterator.next()
            return current
        except TableHTMLParser.HTMLParseError,e:            
            raise functions.OperatorError(__name__.rsplit('.')[-1],e)

        
    def close(self):
        self.ufile.close()
        

def linkiter(source,consume):
    for inp in source:
        for out in consume(inp):
                yield out



def Source():
    return vtbase.VTGenerator(WebTable)


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
        