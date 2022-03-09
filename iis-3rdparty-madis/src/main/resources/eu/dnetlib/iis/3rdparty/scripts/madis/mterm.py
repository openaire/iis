#!/usr/bin/env python

import os
import sys
from importlib import reload
from builtins import input


import functions
import re
import apsw
import traceback
import json
import math
import random
import readline


from os import environ


class MyCompleter(object):  # Custom completer

    def __init__(self, options):
        self.options = sorted(options)

    def complete(self, text, state):
        if state == 0:  # on first trigger, build possible matches
            if not text:
                self.matches = self.options[:]
            else:
                self.matches = [s for s in self.options
                                if s and s.startswith(text)]

        # return match indexed by state
        try:
            return self.matches[state]
        except IndexError:
            return None

    def display_matches(self, substitution, matches, longest_match_length):
        line_buffer = readline.get_line_buffer()
        columns = environ.get("COLUMNS", 80)

        print()

        tpl = "{:<" + str(int(max(map(len, matches)) * 1.2)) + "}"

        buffer = ""
        for match in matches:
            match = tpl.format(match[len(substitution):])
            if len(buffer + match) > columns:
                print(buffer)
                buffer = ""
            buffer += match

        if buffer:
            print(buffer)

        print("> ", end="")
        print(line_buffer, end="")
        sys.stdout.flush()







pipedinput=not sys.stdin.isatty()
errorexit = True
nobuf = False



if pipedinput:
    # If we get piped input use dummy readline
    readline=lambda x:x
    readline.remove_history_item=lambda x:x
    readline.read_history_file=lambda x:x
    readline.write_history_file=lambda x:x
    readline.set_completer=lambda x:x
    readline.add_history=lambda x:x
    readline.parse_and_bind=lambda x:x
    readline.set_completer_delims=lambda x:x
else:
    # Workaround for absence of a real readline module in win32
    import lib.reimport
    if sys.platform == 'win32':
        import pyreadline as readline
    else:
        import readline

import datetime
import time
import locale
import os

from lib.dsv import writer
import csv

try:
    if pipedinput:
        raise Exception('go to except')

    import lib.colorama as colorama
    from colorama import Fore, Back, Style
    colnums = True
except:
    colorama=lambda x:x
    def dummyfunction():
        pass
    colorama.deinit = colorama.init = dummyfunction
    Fore = Style = Back = dummyfunction
    Fore.RED = Style.BRIGHT = Style.RESET_ALL = ''
    colnums = False
    pass

DELIM = Fore.RED+Style.BRIGHT+'|'+Style.RESET_ALL

class mtermoutput(csv.Dialect):
    def __init__(self):
        self.delimiter='|'
        if not allquote:
            self.quotechar='|'
            self.quoting=csv.QUOTE_MINIMAL
        else:
            self.quotechar='"'
            self.quoting=csv.QUOTE_NONNUMERIC
        self.escapechar="\\"
        self.lineterminator='\n'

def createConnection(db):

    try:
        if 'SQLITE_OPEN_URI' in apsw.__dict__:
            connection = functions.Connection(db, flags=apsw.SQLITE_OPEN_READWRITE | apsw.SQLITE_OPEN_CREATE | apsw.SQLITE_OPEN_URI)
        else:
            connection = functions.Connection(db)
        functions.register(connection)
        connection.enableloadextension(True)

    except Exception as e:
        exitwitherror(e)

    # Change TEMP store to where the mterm is run from
    try:
        connection.cursor().execute("PRAGMA temp_store_directory='.';PRAGMA page_size=16384;PRAGMA default_cache_size=3000;")
        # if pipedinput:
    except:
        pass

    functions.settings['stacktrace'] = True

    return connection



def reloadfunctions():
    global connection, automatic_reload, db
    if not automatic_reload:
        return

    modified=lib.reimport.modified()
    if len(modified)==0 or (modified==['__main__']):
        return

    tmp_settings=functions.settings
    tmp_vars=functions.variables
    connection.close()
    try:
        lib.reimport.reimport(*[x for x in modified if x != '__main__'])
    except ValueError:
        pass

    try:
        lib.reimport.reimport('functionslocal')
    except ValueError:
        pass

    connection = createConnection(db)
    functions.settings=tmp_settings
    functions.variables=tmp_vars

def raw_input_no_history(*args):
    global pipedinput

    if pipedinput:
        try:
            input2 = input()
        except EOFError:
            connection.close()
            exit(0)
        return input

    try:
        input2 = input(*args)
    except:
        return None
    if input2!='':
        try:
            readline.remove_history_item(readline.get_current_history_length()-1)
        except:
            pass
    return input2

def update_tablelist():
    global alltables, alltablescompl, connection
    alltables=[]
    alltablescompl=[]
    cursor = connection.cursor()
    cexec=cursor.execute('PRAGMA database_list;')
    for row in cexec:
        cursor1 = connection.cursor()
        if row[1]=='temp':
            cexec1 = cursor1.execute("select name from sqlite_temp_master where type='table';")
        else:
            cexec1 = cursor1.execute("select name from "+row[1]+".sqlite_master where type='table';")

        for row1 in cexec1:
            tname=row1[0].lower()
            if row[1] in ('main', 'temp'):
                alltables.append(tname)
                alltablescompl.append(tname)
            else:
                dbtname=(row[1]+'.'+tname).lower()
                alltables.append(dbtname)
                alltablescompl.append(dbtname)
                if tname not in alltablescompl:
                    alltablescompl.append(tname)
        cursor1.close()
    cursor.close()

def get_table_cols(t):
    global connection

    if '.' in t:
        ts=t.split('.')
        dbname=ts[0]
        tname='.'.join(ts[1:])
    else:
        dbname='main'
        tname=t
    cursor = connection.cursor()
    if dbname=='main':
        cexec=cursor.execute('pragma table_info('+str(tname)+')')
        cols=[x[1] for x in cexec]
    else:
        cexec=cursor.execute('select * from '+str(tname))
        cols=[x[0] for x in cursor.getdescriptionsafe()]
    return cols

def sizeof_fmt(num, use_kibibyte=False):
    base, infix = [(1000.,''),(1024.,'i')][use_kibibyte]
    for x in ['','K%s'%infix,'M%s'%infix,'G%s'%infix]:
        if num < base and num > -base:
            if x == '':
                return str(int(num))
            else:
                return "%3.1f%s" % (num, x)
        num /= base
    return "%3.1f %s" % (num, 'T%sB'%infix)

def approx_rowcount(t):
    timer = time.time()
    minrowid = list(connection.cursor().execute('select min(_rowid_) from ' + t, parse=False))[0][0]
    maxrowid = list(connection.cursor().execute('select max(_rowid_) from ' + t, parse=False))[0][0]
    if maxrowid is None or minrowid is None:
        return 0
    idrange = maxrowid - minrowid + 1
    if (time.time() - timer) > 0.5:
        return idrange
    samplesize = min(int(math.sqrt(idrange)), 100)
    if samplesize == 0:
        return 0
    step = idrange / samplesize
    sample = list(range(random.randrange(0, step) + minrowid, maxrowid + 1, step))
    samplesize = len(sample)
    samplehits = 0
    samplestep = 1
    timer = time.time()
    while sample != []:
        samplehits += list(connection.cursor().execute(
            'select count(*) from ' + t +
            ' where _rowid_ in (' + ','.join([str(x) for x in sample[:samplestep]]) + ');', parse=False))[0][0]
        sample = sample[samplestep:]
        samplestep *= 2
        estimt = samplesize * (time.time() - timer) / (samplesize - len(sample))
        if estimt > 0.5:
            return idrange
    return int(idrange * float(samplehits) / samplesize)


def update_cols_for_table(t):
    global alltablescompl, colscompl, lastcols, connection, updated_tables
    if t!='':
        if t[-1]=='.':
            t=t[0:-1]
        if t[-2:]=='..':
            t=t[0:-2]

    if t in alltablescompl and t not in updated_tables:
        try:
            cols=get_table_cols(t)
            updated_tables.add(t)
            colscompl+= ['.'.join([ t, x ]) for x in cols]
            colscompl+= [x for x in cols]
            colscompl+=[t+'..']
        except:
            pass
        try:
            if '.' in t:
                ts=t.split('.')
                dbname=ts[0]
                tname='.'.join(ts[1:])
            else:
                dbname='main'
                tname=t
            cursor = connection.cursor()
            cexec=cursor.execute('select * from '+dbname+".sqlite_master where type='index' and tbl_name='"+str(tname)+"'")
            icompl= [x[1] for x in cexec]
            colscompl+= ['.'.join([ t, x ]) for x in icompl]
            colscompl+= icompl
        except:
            pass
        try:
            colscompl=list(set(colscompl)-set(lastcols))
        except:
            pass

_update_cols_from_tables_last_text=''
def update_cols_from_tables_in_text(t):
    global alltablescompl, _update_cols_from_tables_last_text

    if t==_update_cols_from_tables_last_text:
        return

    _update_cols_from_tables_last_text=t

    stablesreg='|'.join( (x.replace('$','\$').replace('.', '\.') for x in sorted(alltablescompl, key=len, reverse=True)) )
    foundtables=re.findall(r'[^a-zA-Z0-9_$]('+stablesreg+r')[,.\s)]', t+' ')

    for i in foundtables:
        update_cols_for_table(i)


def mcomplete(textin,state):
    global number_of_kb_exceptions
    number_of_kb_exceptions = 0

    def normalizename(col):
        if re.match(r'\.*[\w_$\d.]+\s*$', col,re.UNICODE):
            return col
        else:
            return "`"+col.lower()+"`"

    def numberedlist(c):
        maxcolchars=len(str(len(c)+1))
        formatstring='{:'+'>'+str(maxcolchars)+'}'
        o=[]
        for num in range(len(c)):
            o.append( formatstring.format(num+1)+'|'+c[num] )
        return o

    text=textin

    #Complete \t to tabs
    if text[-2:]=='\\t':
        if state==0: return text[:-2]+'\t'
        else: return

    prefix=''

    localtables=[]
    completions=[]

    linebuffer=readline.get_line_buffer()

    beforecompl= linebuffer[0:readline.get_begidx()]

    # Only complete '.xxx' completions when space chars exist before completion
    if re.match(r'\s*$', beforecompl):
        completions+=dotcompletions
    # If at the start of the line, show all tables
    if beforecompl=='' and text=='':
        localtables=alltables[:]

        # Check if all tables start with the same character
        if localtables!=[]:
            prefcharset=set( (x[0] for x in localtables) )
            if len(prefcharset)==1:
                localtables+=[' ']
        completions=localtables
    # If completion starts at a string boundary, complete from local dir
    elif beforecompl!='' and beforecompl[-1] in ("'", '"'):
        completions=os.listdir(os.getcwd())
        hits=[x for x in completions if x[:len(text)]==str(text)]
        if state<len(hits):
            return hits[state]
        else: return
    # Detect if in simplified 'from' or .schema
    elif re.search(r'(?i)(from\s(?:\s*[\w\d._$]+(?:\s*,\s*))*(?:\s*[\w\d._$]+)?$)|(^\s*\.schema)|(^\s*\.t)|(^\s*\.tables)', beforecompl, re.DOTALL| re.UNICODE):
        localtables=alltablescompl[:]
        completions=localtables
    else:
        localtables=alltablescompl[:]
        completions+=lastcols+colscompl
        completions+=sqlandmtermstatements+allfuncs+localtables

    hits= [x.lower() for x in completions if x.lower()[:len(text)]==str(text.lower())]

    update_cols_from_tables_in_text(linebuffer)

    if hits==[] and text.find('.')!=-1 and re.match(r'[\w\d._$]+', text):
        tablename=re.match(r'(.+)\.', text).groups()[0].lower()
        update_cols_for_table(tablename)
        hits= [x.lower() for x in colscompl if x.lower()[:len(text)]==str(text.lower())]


    # If completing something that looks like a table, complete only from cols
    if hits==[] and text[-2:]!='..':
        prepost=re.match(r'(.+\.)([^.]*)$', text)
        if prepost:
            prefix, text=prepost.groups()
            hits= [x.lower() for x in lastcols+[y for y in colscompl if y.find('.')==-1] if x.lower()[:len(text)]==str(text.lower())]
            # Complete table.number
            if len(hits) == 0 and text.isdigit():
                cols= get_table_cols(prefix[:-1])
                colnum = int(text)
                if 0 < colnum <= len(cols):
                    hits = [cols[colnum-1]]
                elif colnum == 0:
                    hits = numberedlist(cols)
                    if state<len(hits):
                        return hits[state]
                    else: return

    try:
        # Complete from colnums
        icol=int(text)
        if len(hits)==0 and text.isdigit() and icol>=0:
            # Show all tables when completing 0
            if icol==0 and newcols!=[]:
                if len(newcols)==1:
                    if state>0: return
                    return prefix+normalizename(newcols[0])
                hits = numberedlist(newcols)
                if state<len(hits):
                    return hits[state]
                else: return
            # Complete from last seen when completing for other number
            if icol<=len(lastcols) and lastcols!=[] and state<1:
                return prefix+normalizename(lastcols[icol-1])
    except:
        pass

    if state<len(hits):
        sqlstatem=set(sqlandmtermstatements)
        altset=set(localtables)

        if hits[state]=='..':
            if text=='..' and lastcols!=[]:
                return prefix+', '.join([normalizename(x) for x in lastcols])+' '
            else:
                return prefix+hits[state]
        if hits[state] in sqlstatem:
            return prefix+hits[state]
        if hits[state] in colscompl:
            if text[-2:]=='..':
                tname=text[:-2]
                try:
                    cols=get_table_cols(tname)
                    return prefix+', '.join(cols)+' '
                except:
                    pass
        if hits[state] in altset:
            if text in altset:
                update_cols_for_table(hits[state])
            return prefix+hits[state]
        else:
            return prefix+normalizename(hits[state])
    else:
        return

def buildrawprinter(separator):
    return writer(sys.stdout, dialect=mtermoutput(), delimiter=str(separator))

def schemaprint(cols):
    global pipedinput

    if pipedinput:
        return

    if cols!=[]:
        sys.stdout.write(Style.BRIGHT+'--- '+Style.NORMAL+ Fore.RED+'['+Style.BRIGHT+'0'+Style.NORMAL+'|'+Style.RESET_ALL+Style.BRIGHT+'Column names '+'---'+Style.RESET_ALL+'\n')
        colschars=0
        i1=1
        for i in cols:
            colschars+=len(i)+len(str(i1))+3
            i1+=1
        if colschars<=80:
            i1=1
            for i in cols:
                sys.stdout.write(Fore.RED+'['+Style.BRIGHT+str(i1)+Style.NORMAL+'|'+Style.RESET_ALL+i+' ')
                i1+=1
            sys.stdout.write('\n')
        else:
            totalchars=min(colschars/80 +1, 12) * 80
            mincolchars=12
            colschars=0
            i1=1
            for i in cols:
                charspercolname=max((totalchars-colschars)/(len(cols)+1-i1)-5, mincolchars)
                colschars+=min(len(i), charspercolname)+len(str(i1))+3
                if len(i)>charspercolname and len(cols)>1:
                    i=i[0:int(charspercolname)-1]+'..'
                else:
                    i=i+' '
                sys.stdout.write(Fore.RED+'['+Style.BRIGHT+str(i1)+Style.NORMAL+'|'+Style.RESET_ALL+i)
                i1+=1
            sys.stdout.write('\n')

def printrow(row):
    global rawprinter, colnums
    #print("lala",connection.openiters)
    if not colnums:
        rawprinter.writerow(row)
        return
    rowlen=len(row)
    i1=1
    for d in row:
        if rowlen>3:
            if i1==1:
                sys.stdout.write(Fore.RED+Style.BRIGHT+'['+'1'+'|'+Style.RESET_ALL)
            else:
                sys.stdout.write(Fore.RED+'['+str(i1)+Style.BRIGHT+'|'+Style.RESET_ALL)
        else:
            if i1!=1:
                sys.stdout.write(Fore.RED+Style.BRIGHT+'|'+Style.RESET_ALL)
        if type(d) in (int, float, int):
            d=str(d)
        if isinstance(d,bytes):
            d = d.decode('utf-8')
        elif d is None:
            d=Style.BRIGHT+'null'+Style.RESET_ALL
        try:
            sys.stdout.write(d)
        except KeyboardInterrupt:
            raise
        except:

            sys.stdout.write(d)

        i1+=1
    sys.stdout.write('\n')

def printterm(*args, **kwargs):
    global pipedinput

    msg=','.join([str(x) for x in args])

    if not pipedinput:
        print(msg)

def exitwitherror(*args):
    msg=','.join([str(x) for x in args])

    if errorexit:
        print()
        sys.exit(msg)
    else:
        print((json.dumps({"error":msg}, separators=(',',':'), ensure_ascii=False)))
        print()
        sys.stdout.flush()

def process_args():
    global connection, functions, errorexit, db, nobuf

    args = sys.argv[1:]

    # Continue on error when input is piped in
    if len(args) >= 1 and pipedinput:
        setargs = set(args)
        if '-bailoff' in setargs or '-coe' in setargs:
            try:
                setargs.remove('-bailoff')
            except KeyError:
                pass

            try:
                setargs.remove('-coe')
            except KeyError:
                pass

            errorexit = False

        if '-nobuf' in setargs:
            setargs.remove('-nobuf')
            nobuf = True

        args = list(setargs)

    if len(args) >= 1:
        db = args[-1]
        if db == "-q":
            db = ':memory:'

    connection = createConnection(db)


    if db == '' or db == ':memory:':
        functions.variables.execdb = None
    else:
        functions.variables.execdb = str(os.path.abspath(os.path.expandvars(os.path.expanduser(os.path.normcase(db)))))

    # Found query in args
    if len(args)>2:
        st = ' '.join(args[2:])


        c = connection.cursor()
        try:
            for r in c.execute(st):
                rawprinter.writerow(r)
            c.close()
        except KeyboardInterrupt:
            sys.exit()
        finally:
            try:
                c.close()
            except:
                pass



VERSION='1.0'
mtermdetails="mTerm - version "+VERSION
intromessage="""Enter ".help" for instructions
Enter SQL statements terminated with a ";" """

helpmessage=""".functions             Lists all functions
.help                  Show this message (also accepts '.h' )
.help FUNCTION         Show FUNCTION's help page
.quit                  Exit this program
.schema TABLE          Show the CREATE statements
.quote                 Toggle between normal quoting mode and quoting all mode
.beep                  Make a sound when a query finishes executing
.tables                List names of tables (you can also use ".t" or double TAB)
.t TABLE               Browse table
.explain               Explain query plan
.colnums               Toggle showing column numbers
.separator SEP         Change separator to SEP. For tabs use 'tsv' or '\\t' as SEP
                       Separator is used only when NOT using colnums
.vacuum                Vacuum DB using a temp file in current path
.queryplan query       Displays the queryplan of the query

Use: FILE or CLIPBOARD function for importing data
     OUTPUT or CLIPOUT function for exporting data"""

if 'HOME' not in os.environ: # Windows systems
        if 'HOMEDRIVE' in os.environ and 'HOMEPATH' in os.environ:
                os.environ['HOME'] = os.path.join(os.environ['HOMEDRIVE'], os.environ['HOMEPATH'])
        else:
                os.environ['HOME'] = "C:\\"

histfile = os.path.join(os.environ["HOME"], ".mterm")



automatic_reload=False
if not pipedinput:
    try:
        readline.read_history_file(histfile)
    except IOError:
        pass
    import atexit

    atexit.register(readline.write_history_file, histfile)
    automatic_reload=True
    readline.set_completer(mcomplete)
    readline.parse_and_bind("tab: complete")
    readline.set_completer_delims(' \t\n`!@#$^&*()=+[{]}|;:\'",<>?')





separator = "|"
allquote = False
beeping = False
db = ""
language, output_encoding = locale.getdefaultlocale()



if output_encoding==None:
    output_encoding='UTF8'


functions.variables.flowname='main'

rawprinter=buildrawprinter(separator)

process_args()

sqlandmtermstatements=['select ', 'create ', 'where ', 'table ', 'group by ', 'drop ', 'order by ', 'index ', 'from ', 'alter ', 'limit ', 'delete ', '..',
    "attach database '", 'detach database ', 'distinct', 'exists ']
dotcompletions=['.help ', '.colnums', '.schema ', '.functions ', '.tables', '.explain ', '.vacuum', '.queryplan ']
allfuncs=list(functions.functions['vtable'].keys())+list(functions.functions['row'].keys())+list(functions.functions['aggregate'].keys())
alltables=[]
alltablescompl=[]
updated_tables=set()
update_tablelist()
lastcols=[]
newcols=[]
colscompl=[]

#Intro Message
if not pipedinput:
    print(mtermdetails)
    print("running on Python: "+'.'.join([str(x) for x in sys.version_info[0:3]])+', APSW: '+apsw.apswversion()+', SQLite: '+apsw.sqlitelibversion(), end=' ')
    try:
        sys.stdout.write(", madIS: "+functions.VERSION+'\n')
    except:
        print()
    print(intromessage)

number_of_kb_exceptions=0

while True:

    statement = raw_input_no_history("mterm> ")
    if statement==None:
        number_of_kb_exceptions+=1
        print()
        if number_of_kb_exceptions<2:
            continue
        else:
            readline.write_history_file(histfile)
            break

    #Skip comments
    statement = str(statement)
    if statement.startswith('--'):
        continue

    number_of_kb_exceptions=0

    #scan for commands
    iscommand=re.match("\s*\.(?P<command>\w+)\s*(?P<argument>([\w\.]*))(?P<rest>.*)$", statement)
    validcommand=False
    queryplan = False

    if iscommand:
        validcommand=True
        command=iscommand.group('command')
        argument=iscommand.group('argument')
        rest=iscommand.group('rest')
        origstatement=statement
        statement=None

        if command=='separator':
            tmpseparator=separator
            if argument=='csv':
                separator = ","
            elif argument in ('tsv','\\t','\t'):
                separator = "\t"
            else:
                separator = argument

            if separator == '':
                colnums = True
                separator = '|'

            if separator!=tmpseparator:
                colnums = False
                rawprinter=buildrawprinter(separator)

        elif command=='explain':
            statement=re.sub("^\s*\.explain\s+", "explain query plan ", origstatement)

        elif command=='queryplan':
            try:
                statement = re.match(r"\s*\.queryplan\s+(.+)", origstatement).groups()[0]
                queryplan = True
            except IndexError:
                pass

        elif command=='quote':
            allquote^=True
            if allquote:
                printterm("Quoting output, uncoloured columns")
                colnums=False
            else:
                printterm("Not quoting output, coloured columns")
                colnums=True
            rawprinter=buildrawprinter(separator)

        elif command=='beep':
            beeping^=True
            if beeping:
                printterm("Beeping enabled")
            else:
                printterm("Beeping disabled")

        elif command=='colnums':
            colnums^=True
            if colnums:
                printterm("Colnums enabled")
            else:
                printterm("Colnums disabled")

        elif 'tables'.startswith(command):
            update_tablelist()
            argument=argument.rstrip('; ')
            if not argument:
                maxtlen = 0
                for i in sorted(alltables):
                    maxtlen = max(maxtlen, len(i))
                maxtlen += 1

                for i in sorted(alltables):
                    l = ('{:<' + str(maxtlen) + '}').format(i)

                    try:
                        l += DELIM + " cols:{:<4}".format(str(len(get_table_cols(i))))
                    except KeyboardInterrupt:
                        print()
                        break
                    except:
                        pass

                    try:
                        l += DELIM + " ~rows:" + sizeof_fmt(approx_rowcount(i))
                    except KeyboardInterrupt:
                        print()
                        break
                    except:
                        pass

                    printterm(l)
            else:
                statement = 'select * from '+argument+' limit 2;'

        elif 'select'.startswith(command):

            update_tablelist()
            argument = argument.rstrip('; ')
            if not argument:
                for i in sorted(alltables):
                    printterm(i)
            else:
                statement = 'select * from '+argument + ';'

        elif command=='vacuum':
            statement="PRAGMA temp_store_directory = '.';VACUUM;PRAGMA temp_store_directory = '';"

        elif command=='schema':
            if not argument:
                statement="select sql from (select * from sqlite_master union all select * from sqlite_temp_master) where sql is not null;"
            else:
                argument=argument.rstrip('; ')
                update_tablelist()
                if argument not in alltables:
                    printterm("No table found")
                else:
                    db='main'
                    if '.' in argument:
                        sa=argument.split('.')
                        db=sa[0]
                        argument=''.join(sa[1:])
                    statement="select sql from (select * from "+db+".sqlite_master union all select * from sqlite_temp_master) where tbl_name like '%s' and sql is not null;" %(argument)

        elif "quit".startswith(command):
            connection.close()
            exit(0)

        elif command=="functions":
            for ftype in functions.functions:
                for f in functions.functions[ftype]:
                    printterm(f+' :'+ftype)

        elif "help".startswith(command):
            if not argument:
                printterm(helpmessage)
            else:
                for i in functions.functions:
                    if argument in functions.functions[i]:
                        printterm("Function "+ argument + ":")
                        printterm(functions.mstr(functions.functions[i][argument].__doc__))

        elif command=="autoreload":
            automatic_reload=automatic_reload ^ True
            printterm("Automatic reload is now: " + str(automatic_reload))

        else:
            validcommand=False
            printterm("""unknown command. Enter ".help" for help""")

        if validcommand:
            histstatement='.'+command+' '+argument+rest
            try:
                readline.add_history(histstatement)
            except:
                pass

    if statement:

        histstatement=statement
        while not apsw.complete(statement):
            more = raw_input_no_history('  ..> ')
            if more==None:
                statement=None
                break
            statement = statement + '\n' + more
            histstatement=histstatement+' '+more

        reloadfunctions()
        number_of_kb_exceptions=0
        if not statement:
            printterm()
            continue
        try:
            if not validcommand:
                readline.add_history(histstatement)
        except:
            pass


        before = datetime.datetime.now()
        try:
            if queryplan:
                cexec = connection.queryplan(statement)
                desc = next(cexec)
            else:
                cursor = connection.cursor()
                cexec = cursor.execute(statement)
                try:
                    desc = cursor.getdescriptionsafe()
                    lastcols[0:len(desc)] = [x for x, y in desc]
                except apsw.ExecutionCompleteError as e:
                    desc = []

            newcols=[x for x,y in desc]

            colorama.init()
            rownum=0

            if not pipedinput:
                for row in cexec:
                    printrow(row)
                    rownum+=1
            else:
                print((json.dumps({"schema":desc}, separators=(',',':'), ensure_ascii=False)))
                for row in cexec:
                    print((json.dumps(row, separators=(',',':'), ensure_ascii=False)))
                    if nobuf:
                        sys.stdout.flush()
                print()
                sys.stdout.flush()

            if not queryplan:
                cursor.close()


            after=datetime.datetime.now()
            tmdiff=after-before

            schemaprint(newcols)
            if not pipedinput:
                if rownum==0:
                    printterm( "Query executed in %s min. %s sec %s msec." %((int(tmdiff.days)*24*60+(int(tmdiff.seconds)/60),(int(tmdiff.seconds)%60),(int(tmdiff.microseconds)/1000))) )
                else:
                    print("Query executed and displayed %s"%(rownum), end=' ')
                    if rownum==1: print("row", end=' ')
                    else: print("rows", end=' ')
                    print("in %s min. %s sec %s msec." %((int(tmdiff.days)*24*60+(int(tmdiff.seconds)/60),(int(tmdiff.seconds)%60),(int(tmdiff.microseconds)/1000))))
            if beeping:
                printterm('\a\a')

            colscompl=[]
            updated_tables=set()

            #Autoupdate in case of schema change
            if re.search(r'(?i)(create|attach|drop)', statement):
                update_tablelist()

        except KeyboardInterrupt:
            print()
            schemaprint(newcols)
            printterm("KeyboardInterrupt exception: Query execution stopped", exit=True)
            continue
        except (apsw.SQLError, apsw.ConstraintError , functions.MadisError) as e:
            emsg=str(e)
            if pipedinput:
                exitwitherror(functions.mstr(emsg))
            else:
                try:
                    if 'Error:' in emsg:
                        emsgsplit=emsg.split(':')
                        print(Fore.RED+Style.BRIGHT+ emsgsplit[0] +':'+Style.RESET_ALL+ ':'.join(emsgsplit[1:]))
                    else:
                        print(e)
                except:
                    print(e)

            continue
        except Exception as e:
            trlines = []

            for i in reversed(traceback.format_exc(limit=sys.getrecursionlimit()).splitlines()):
                trlines.append(i)
                if i.strip().startswith('File'):
                    break

            msg=Fore.RED+Style.BRIGHT+"Unknown error:" + Style.RESET_ALL + "\nTraceback is:\n" + '\n'.join(reversed(trlines))
            if pipedinput:
                exitwitherror(functions.mstr(msg))
            else:
                print(msg)

        finally:
            colorama.deinit()
            try:
                cursor.close()
            except:
                #print "Not proper clean-up"
                pass

