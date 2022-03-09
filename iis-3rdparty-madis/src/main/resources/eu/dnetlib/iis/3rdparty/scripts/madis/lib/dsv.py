import csv
import codecs
import io

"""
Differences from csv.reader module
Default dialect: SQLITE TABS DUMP
encoding parameter (default utf-8)
delimiter can be a multicaharacter string - given as extra parameter , not in dialect
......BUT IF YOUR FILE HAS THE chr(30) character(apart the delimiter) it will be disaster (or if encoding is multibyte )!!!!!!!!
"""

#many string delimiter!!!

class sqlitedmp(csv.Dialect):
    def __init__(self):
        self.delimiter="\t"
        #self.doublequote=True
        self.quotechar=None
        self.quoting=csv.QUOTE_NONE
        #self.quotechar='"'
        #self.quoting=csv.QUOTE_MINIMAL
        self.lineterminator='\n'

SQLITE_DIALECT=sqlitedmp()

class Onedel:
    def __init__(self,reader,big,one):
        self.reader=reader
        self.big=big
        self.one=one
    def __next__(self):
        return self.reader.next().replace(self.big,self.one)
    def __iter__(self):
        return self

#delimiter seperated values


class writer:
    """
    A CSV writer with default dialect sqlite dump files and utf8 encoding, NO multicharacter delimiter
    """
    def __init__(self,tsvfile,dialect=SQLITE_DIALECT,encoding="utf_8",**kwds):
        self.writer=UnicodeWriter(tsvfile,dialect,encoding,**kwds)
    def writerow(self,row):
        self.writer.writerow(row)
    def writerows(self,rows):
        self.writer.writerows(rows)

class reader:
    """
    A CSV reader which will iterate over lines in the CSV file "tsvfile",
    which is encoded in the given encoding.
    (with default dialect sqlite dump files and utf8 encoding, multicharacter delimiter YES)
    """
    def __init__(self,tsvfile,hasheader=False,dialect=SQLITE_DIALECT,encoding="utf_8",**kwds):
        self.hasheader=hasheader
        self.fast = False
        if not hasheader:
            self.reader=UnicodeReader(tsvfile,dialect,encoding,**kwds)
        else:
            self.reader=UnicodeDictReader(tsvfile,dialect,encoding,**kwds)

    def __iter__(self):
        return self.reader

    def fieldnames(self):
        return None

class UTF8Recoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, f, encoding):
        self.reader = codecs.iterdecode(f, encoding, 'replace')
        self.encoding = encoding.lower()
        self.f = f

    def __iter__(self):
        # Shortcircuit for default case
        if self.encoding == 'utf_8':
            return self.f
        return self

    def __next__(self):
        return self.reader.next().encode("utf_8")

class UnicodeReader:
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    To accept multicharacter delimiters a temporal replacement with ~ character happens
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf_8", **kwds):
        f = UTF8Recoder(f, encoding)
        self.replace=False
        if 'delimiter' in kwds and len(kwds['delimiter'])>1:
            self.replace=True
            self.mdel=chr(30)
            self.big=kwds['delimiter']
            kwds['delimiter']=self.mdel
            self.reader = csv.reader(codecs.iterdecode(Onedel(f,self.big,self.mdel), 'utf-8'),dialect=dialect, **kwds)
            #self.reader = csv.reader(Onedel(f,self.big,self.mdel), dialect=dialect, **kwds)
            self.next = self.nextwithreplace
        else:
            self.reader = csv.reader(codecs.iterdecode(f, 'utf-8'),dialect=dialect, **kwds)
            self.reader = csv.reader(f, dialect=dialect, **kwds)

    def __next__(self):
        return [s for s in next(self.reader)]

    def nextwithreplace(self):
        return [str(s.replace(self.mdel,self.big), "utf_8") for s in next(self.reader)]

    def __iter__(self):
        return self

    def fieldnames(self):
        return None

class UnicodeDictReader:
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    To accept multicharacter delimiters a temporal replacement with ~ character happens
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf_8", **kwds):
        f = UTF8Recoder(f, encoding)
        self.replace=False
        if 'delimiter' in kwds and len(kwds['delimiter'])>1:
            self.replace=True
            self.mdel=chr(30)
            self.big=kwds['delimiter']
            kwds['delimiter']=self.mdel
            self.reader = csv.reader(Onedel(f,self.big,self.mdel), dialect=dialect, **kwds)
        else:
            self.reader = csv.reader(f, dialect=dialect, **kwds)
        self.fields=None

    def __readheader(self):
        if not self.fields:
            row = next(self.reader)
            self.fields=[str(s, "utf_8") for s in row]
            
    def __next__(self):
        if not self.fields:
            self.__readheader()
        row = next(self.reader)
        rowdict=dict()
        if self.replace:
            for field,cell in zip(self.fields,row):
                rowdict[field]=str(cell.replace(self.mdel,self.big), "utf_8")
        else:
            for field,cell in zip(self.fields,row):
                rowdict[field]=str(cell, "utf_8")
        
        return rowdict

    def fieldnames(self):
        if not self.fields:
            self.__readheader()
        return self.fields

    def __iter__(self):
        return self

def anytouni(i):
    if i is None:
        return 'null'
    if isinstance(i,str):
        return str(i)
    elif not isinstance(i,str):
        return str(repr(i))
    else:
        return i
    return unirow




##one delimiter only
class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf_8", **kwds):
        # Redirect output to a queue
        self.queue = io.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([anytouni(s).encode("utf_8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf_8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)




def main():
    import sys
    import time    
    fname='partheaders.tsv'
    print("Test file %s" %(fname))
    print(time.strftime("%Y-%m-%d %H:%M:%S"),"\tBEGIN", file=sys.stderr)

    with open('partheaders.tsv') as f:
        p=reader(f,hasheader=True)
        for line in p:
            print(line)
    print(time.strftime("%Y-%m-%d %H:%M:%S"),"\tEND", file=sys.stderr)



if __name__ == "__main__":    
    main()
    
