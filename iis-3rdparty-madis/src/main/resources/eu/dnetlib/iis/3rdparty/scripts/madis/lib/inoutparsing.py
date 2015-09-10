import urllib2
import tempfile
import csv
import re
import os
from lib.boolops import xor

# Set maximum field size to 20MB
csv.field_size_limit(20000000)

class defaultcsv(csv.Dialect):
    def __init__(self):
        self.delimiter=','
        self.doublequote=True
        self.quotechar='"'
        self.quoting=csv.QUOTE_MINIMAL
        self.lineterminator='\n'

class tsv(csv.Dialect):
    def __init__(self):
        self.delimiter='\t'
        self.doublequote=True
        self.quotechar='"'
        self.quoting=csv.QUOTE_MINIMAL
        self.lineterminator='\n'


class line(csv.Dialect):
    def __init__(self):
        self.delimiter='\n'
        self.doublequote=False
        self.quotechar='"'
        self.quoting=csv.QUOTE_NONE
        self.lineterminator='\n'

urllike=re.compile('^((?:http(?:s)?|ftp)://)(?:(?:[A-Z0-9]+(?:-*[A-Z0-9]+)*\.)+[A-Z]{2,6}|localhost|\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})(?::\d+)?(?:/?|/\S+)$', re.IGNORECASE)
urlstart=re.compile('^(http(?:s)?|ftp)', re.IGNORECASE)

boolargs = ['skipinitialspace', 'doublequote']
nonstringargs = {'quoting':{'QUOTE_ALL':csv.QUOTE_ALL, 'QUOTE_NONE':csv.QUOTE_NONE, 'QUOTE_MINIMAL':csv.QUOTE_MINIMAL, 'QUOTE_NONNUMERIC':csv.QUOTE_NONNUMERIC}, 'dialect':{'line':line(),'tsv':tsv(),'csv':defaultcsv(), 'json':'json'}}
needsescape=['delimiter','quotechar','lineterminator']

class InputsError(Exception):
    pass


def inoutargsparse(args,kargs):
    returnvals= {'url':False,'header':False , 'compression':False, 'compressiontype':None, 'filename':None}
    where=None      #This gets registered with the Connection
    if not xor((len(args)>0),('file' in kargs),('url' in kargs),('http' in kargs),('ftp' in kargs),('https' in kargs)):
        raise InputsError()
    if 'http' in kargs:
        kargs['url']='http:'+kargs['http']
        del kargs['http']
    elif 'ftp' in kargs:
        kargs['url']='ftp:'+kargs['ftp']
        del kargs['ftp']
    elif 'https' in kargs:
        kargs['url']='https:'+kargs['https']
        del kargs['https']
    returnvals['url']=False
    if len(args)>0:
        where=args[0]
    elif 'file' in kargs:
        where=kargs['file']
        del kargs['file']
    else: #url
        returnvals['url']=True
        where=kargs['url']
        v=urllike.match(where)
        if v :
            if not v.groups()[0] or v.groups()[0]=='':
                where="http://"+where
        del kargs['url']
    if 'header' in kargs:
        returnvals['header']=kargs['header']
        del kargs['header']

    #########################################################
    if where.endswith('.zip'):
        returnvals['compression']=True
        returnvals['compressiontype']='zip'

    if where.endswith('.gz') or where.endswith('.gzip'):
        returnvals['compression']=True
        returnvals['compressiontype']='gz'

    #########################################################
    if 'compression' in kargs :
        returnvals['compression']=kargs['compression']
        del kargs['compression']
    elif 'compressiontype' in kargs:
        returnvals['compression']=True

    if 'compressiontype' not in kargs:
        returnvals['compressiontype']='zip'
        if where.endswith('.gz') or where.endswith('.gzip'):
            returnvals['compressiontype']='gz'
    else:
        returnvals['compressiontype']=kargs['compressiontype']
        del kargs['compressiontype']

    returnvals['filename']=where
    return returnvals





def cacheurl(url,extraheaders):
    fd , fname =tempfile.mkstemp(suffix="kill.urlfetch")
    os.close(fd)
    req=urllib2.Request(url,None,extraheaders)
    #urliter=urllib2.urlopen(url)
    urliter=urllib2.urlopen(req)
    tmp=open(fname,"wb")
    for line in urliter:
        tmp.write(line)
    tmp.close()
    urliter.close()
    return fname
