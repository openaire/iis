
"""
.. function:: output formatting_options 'filename' query

Writes in *filename* the output of *query* formatted according to *formatting* options.

:Returned table schema:
    - *return_value* int
        Boolean value 1 indicating success. On failure an exception is thrown.

Formatting options:

.. toadd html        In html mode table is formatted as an html table TODO ????

:mode:
    - plain     *Default*. The columns are concatened and written together.
    - tsv       Writes data in a tab separated format. *TSV* mode is autoselected when the filename ends in ".tsv".
    - csv       Writes data in a comma separated format. *CSV* mode is autoselected when the filename ends in ".csv".
    - json      Writes data in a line separated JSON format. Header is always added. *JSON* mode is autoselected when the filename ends in ".JSON".
    - db        Writes data in a SQLite DB. *DB* mode is autoselected when the filename ends in ".db".

                - If pagesize:xxxxx option is given, set new DBs page size to parameter given, if not inherit page size from parent DB.

    - gtable    In gtable mode table is formatted as a google Data Table for visualisation.
    - gjson     In gjson mode table is formatted in a json format accepted by google visualisation widgets.

    If *mode* is not *csv* any given csv formatting options are ignored.

:append:
    t/f If true the output is append in the file, ignored in compression mode.

:compression:
    t/f If true the output will be compressed. Default compressor type is *gz*.

:compressiontype:
    gz/zip Selects between the two compression types.

:split:
    (number) It splits the input to many *db* or *json* files. Splitting only works when writting to a *db* or *JSON*. Splitting is done by using the first column of
    the input and it outputs all columns except the first one. If the *split* argument is greater than *1* then the output will
    always be splitted to the defined number of files. If the split argument is 1 or lower, then the output will only contain the parts of which
    a key were found on the first column of the input data.

Detailed description of additional output formating options can be found in :func:`~functions.vtable.file.file` function description.

Examples:

    >>> table1('''
    ... James   10	2
    ... Mark    7	3
    ... Lila    74	1
    ... ''')
    >>> sql("select * from table1")
    a     | b  | c
    --------------
    James | 10 | 2
    Mark  | 7  | 3
    Lila  | 74 | 1
    >>> sql("output file:../../tests/table1.csv delimiter:# header:t select a as name , b as age, c as rank from table1")
    return_value
    ------------
    1
    >>> sql("file file:../../tests/table1.csv delimiter:# header:t")
    name  | age | rank
    ------------------
    James | 10  | 2
    Mark  | 7   | 3
    Lila  | 74  | 1
"""
import os.path

from . import setpath
from .vtout import SourceNtoOne
from lib.dsv import writer
import gzip
from lib.ziputils import ZipIter
import functions
from lib.vtoutgtable import vtoutpugtformat
import lib.inoutparsing
import os
import apsw
import gc
from collections import defaultdict


registered=True

def fileit(p, append=False):
    if append:
        return open(p, "a", buffering=100000)
    return open(p, "w" , buffering=100000)

def getoutput(p, append,compress,comptype):
    source=p
    it=None

    if compress and ( comptype == 'zip'):
        it = ZipIter(source, "w")
    elif compress and (comptype=='gzip' or comptype == 'gz'):
            itt = fileit(source+'.gz')
            it = gzip.GzipFile(mode="w", compresslevel=6, fileobj=itt)
    else:
        it=fileit(source,append)
    return it

def autoext(f, ftype, typelist):
    fname, ext=os.path.splitext(f)
    if ext=='' and ftype in typelist:
        ext=typelist[ftype]
        return fname+'.'+ext
    return f

def autotype(f, extlist):
    fname, ext=os.path.splitext(f)
    if ext!='':
        ext=ext[1:]
        if ext in extlist:
            return extlist[ext]
    return 'plain'

def outputData(diter, schema, connection, *args, **formatArgs):
    ### Parameter handling ###
    where=None
    if len(args)>0:
        where=args[0]
    elif 'file' in formatArgs:
        where=formatArgs['file']
    else:
        raise functions.OperatorError(__name__.rsplit('.')[-1],"No destination provided")

    if 'file' in formatArgs:
        del formatArgs['file']

    if 'mode' not in formatArgs:
        formatArgs['mode']=autotype(where, {'csv':'csv', 'tsv':'tsv', 'xls':'tsv', 'db':'db', 'json':'json'})

    if 'header' not in formatArgs:
        header=False
    else:
        header=formatArgs['header']
        del formatArgs['header']

    if 'compression' not in formatArgs:
       formatArgs['compression']=False
    if 'compressiontype' not in formatArgs:
        formatArgs['compressiontype']='gz'

    orderby = None
    if 'orderby' in formatArgs:
        orderby = formatArgs['orderby']
        del formatArgs['orderby']

    if 'orderbydesc' in formatArgs:
        orderby = formatArgs['orderbydesc'] + ' desc'
        del formatArgs['orderbydesc']

    append=False
    if 'append' in formatArgs:
        append=formatArgs['append']
        del formatArgs['append']

    type2ext={'csv':'csv', 'tsv':'xls', 'plain':'txt', 'db':'db', 'json':'json'}

    where=autoext(where, formatArgs['mode'], type2ext)
    filename, ext=os.path.splitext(os.path.basename(where))
    fullpath=os.path.split(where)[0]

    if not (formatArgs['mode'] == 'db' or (formatArgs['mode']=='json' and 'split' in formatArgs)):
        fileIter=getoutput(where,append,formatArgs['compression'],formatArgs['compressiontype'])

    del formatArgs['compressiontype']
    del formatArgs['compression']
    try:
        
        if formatArgs['mode']=='json':
            del formatArgs['mode']
            import json
            je = json.JSONEncoder(separators = (',',':'), ensure_ascii = True, check_circular = False).encode

            if 'split' in formatArgs:
                def cjs():
                    unikey = str(key)
                    t=open(os.path.join(fullpath, filename+'.'+unikey+ext), 'w')
                    print(je( {'schema':schema[1:]} ), file=t)
                    splitkeys[unikey]=t
                    jsfiles[key]=t
                    # Case for number as key
                    if unikey != key:
                        splitkeys[key] = splitkeys[unikey]
                    return splitkeys[key]

                jsfiles = {}
                splitkeys=defaultdict(cjs)

                gc.disable()
                for row in diter:
                    key=row[0]
                    print(je(row[1:]), file=splitkeys[key])
                gc.enable()

                # Create other parts
                maxparts = 1
                try:
                    maxparts = int(formatArgs['split'])
                except ValueError:
                    maxparts = 1

                if maxparts > 1:
                    for i in range(0, maxparts):
                        if i not in splitkeys:
                            key = i
                            tmp = splitkeys[key]

                for f in list(jsfiles.values()):
                    if f is not None:
                        f.close()
            else:
                fileIter.write(je({'schema':schema}) + '\n')

                for row in diter:
                    print(je(row), file=fileIter)

        elif formatArgs['mode'] == 'csv':
            del formatArgs['mode']
            csvprinter = writer(fileIter, 'excel', **formatArgs)
            if header:
                csvprinter.writerow([h[0] for h in schema])
                
            for row in diter:
                csvprinter.writerow(row)

        elif formatArgs['mode'] == 'tsv':
            del formatArgs['mode']
            csvprinter = writer(fileIter, 'excel-tab', **formatArgs)
            if header:
                csvprinter.writerow([h[0] for h in schema])

            for row in diter:
                csvprinter.writerow([x.replace('\t', '    ') if type(x) is str or type(x) is str else x for x in row])

        elif formatArgs['mode']=='gtable':
            vtoutpugtformat(fileIter,diter,simplejson=False)

        elif formatArgs['mode']=='gjson':
            vtoutpugtformat(fileIter,diter,simplejson=True)

        elif formatArgs['mode']=='html':
            raise functions.OperatorError(__name__.rsplit('.')[-1],"HTML format not available yet")

        elif formatArgs['mode']=='plain':
            for row in diter:
                fileIter.write(((''.join([str(x) for x in row]))+'\n').encode('utf-8'))

        elif formatArgs['mode']=='db':
            def createdb(where, tname, schema, page_size=16384):
                c = apsw.Connection(where)
                cursor = c.cursor()
                list(cursor.execute('pragma page_size='+str(page_size)+';pragma cache_size=-1000;pragma legacy_file_format=false;pragma synchronous=0;pragma journal_mode=OFF;PRAGMA locking_mode = EXCLUSIVE'))
                if orderby:
                    tname = '_' + tname
                    create_schema='create temp table '+tname+'('
                else:
                    create_schema='create table '+tname+'('
                create_schema+='`'+str(schema[0][0])+'`'+ (' '+str(schema[0][1]) if schema[0][1]!=None else '')
                for colname, coltype in schema[1:]:
                    create_schema+=',`'+str(colname)+'`'+ (' '+str(coltype) if coltype!=None else '')
                create_schema+='); begin exclusive;'
                list(cursor.execute(create_schema))
                insertquery="insert into "+tname+' values('+','.join(['?']*len(schema))+')'
                return c, cursor, insertquery

            if 'pagesize' in formatArgs:
                page_size=int(formatArgs['pagesize'])
            else:
                page_size=list(connection.cursor().execute('pragma page_size'))[0][0]
                
            tablename=filename
            if 'tablename' in formatArgs:
                tablename=formatArgs['tablename']

            if 'split' in formatArgs:
                maxparts = 0
                try:
                    maxparts = int(formatArgs['split'])
                except ValueError:
                    maxparts = 0

                # If not split parts is defined
                if maxparts == 0:
                    ns = lambda x:x
                    def cdb():
                        unikey = str(key)
                        t=createdb(os.path.join(fullpath, filename+'.'+unikey+ext), tablename, schema[1:], page_size)
                        splitkeys[unikey]=t[1].execute
                        ns.insertqueryw = t[2]
                        dbcon[key]=t[0], t[1]
                        # Case for number as key
                        if unikey != key:
                            splitkeys[key] = splitkeys[unikey]
                        return splitkeys[key]

                    dbcon = {}
                    splitkeys=defaultdict(cdb)

                    gc.disable()
                    for row in diter:
                        key=row[0]
                        splitkeys[key](ns.insertqueryw, row[1:])
                    gc.enable()

                    for c, cursor in list(dbcon.values()):
                        if c != None:
                            cursor.execute('commit')
                            c.close()
                else:
                # Splitparts defined
                    cursors = []
                    dbcon = []
                    if "MSPW" in functions.apsw_version:
                        iters = []
                        senders = []
                        for i in range(0, maxparts):
                            t = createdb(os.path.join(fullpath, filename+'.'+str(i)+ext), tablename, schema[1:], page_size)
                            it = t[1].executesplit(t[2])
                            iters.append(it)
                            senders.append(it.send)
                            it.send(None)
                            dbcon.append((t[0], t[1]))
                        senders = tuple(senders)

                        for row in diter:
                            senders[hash(row[0]) % maxparts](row)

                        for it in iters:
                            it.close()
                    else:
                        for i in range(0, maxparts):
                            t = createdb(os.path.join(fullpath, filename+'.'+str(i)+ext), tablename, schema[1:], page_size)
                            cursors.append(t[1].execute)
                            dbcon.append((t[0], t[1]))
                            insertqueryw = t[2]
                        cursors = tuple(cursors)

                        for row in diter:
                            cursors[hash(row[0]) % maxparts](insertqueryw, row[1:])

                    for c, cursor in dbcon:
                        if c != None:
                            if orderby:
                                cursor.execute('pragma cache_size=-'+str(100000)+';create table '+tablename+' as select * from _'+tablename+' order by '+orderby)
                            cursor.execute('commit')
                            c.close()
            else:
                # Write to db without split
                c, cursor, insertquery=createdb(where, tablename, schema, page_size)

                gc.disable()
                cursor.executemany(insertquery, diter)
                gc.enable()

                list(cursor.execute('commit'))
                c.close()
        else:
            raise functions.OperatorError(__name__.rsplit('.')[-1],"Unknown mode value")

    except StopIteration as e:
        pass

    try:
        fileIter.close()
    except NameError:
        pass

boolargs=lib.inoutparsing.boolargs+['append','header','compression']


def Source():
    global boolargs, nonstringargs
    return SourceNtoOne(outputData,boolargs, lib.inoutparsing.nonstringargs,lib.inoutparsing.needsescape, connectionhandler=True)


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
