"""
.. function:: clipboard()

Returns the contents of the system's clipboard. If the clipboard's contents are guessed to be a table, then it automatically splits the contents in its output.

:h:
    if the 'h' option is provided to *clipboard()* function, the first row of the clipboard's data is regarded as the schema of the data.

:Returned table schema:
    Column names start from C1... , all column types are text

Examples:

    >>> sql("select * from clipboard()")
    C1   | C2                    | C3          | C4
    ------------------------------------------------------
    1    | Zimbabwe              | 304.30      | 2009 est.
    2    | Japan                 | 192.10      | 2009 est.
    3    | Saint Kitts and Nevis | 185.00      | 2009 est.

"""
import vtbase

registered=True
external_stream=True

class clipboard(vtbase.VT):
    def __init__(self):
        self.schema=[('C1', 'text')]
        self.count = None

    def checkfordelimiter(self, delim = '\t'):
        #check for regular schema
        hasschema=True
        self.count=0
        if len(self.data)>0:
            self.count = self.data[0].count(delim)
            if self.count==0:
                hasschema=False
            else:
                for i in self.data[1:]:
                    if i.count(delim) != self.count:
                        hasschema=False
                        break
        return hasschema

    def VTiter(self, *parsedArgs, **envars):
        largs, dictargs = self.full_parse(parsedArgs)
        import lib.pyperclip as clip
        data=unicode(clip.getcb(), 'utf_8')

        if data.count('\n')>=data.count('\r'):
            data=data.split('\n')
        else:
            data=data.split('\r')

        #delete empty lines from the end
        for i in xrange(len(data)-1,-1,-1):
            if len(data[i])==0:
                del data[i]
            else:
                break

        self.data = data
        delim = None

        if 'delimiter' in dictargs:
            delim = dictargs['delimiter']

            if delim == r'\t':
                delim = '\t'

            if delim == '':
                delim = None

        else:
            if self.checkfordelimiter('\t'):
                delim = '\t'
            elif self.checkfordelimiter(','):
                delim = ','
            elif self.checkfordelimiter(';'):
                delim = ';'
            elif self.checkfordelimiter(':'):
                delim = ':'
            elif self.checkfordelimiter(' ') and len(data)>1:
                delim = ' '

        if delim != None:
            data=[i.split(delim) for i in data]
            self.schema = None
            header = False

            # Check for header directive
            for i in parsedArgs:
                if i.startswith('h'):
                    header = True

            if header and len(data)>0:
                self.schema = [(c,'text') for c in data[0]]
                data = data[1:]
            else:
                if self.count == None:
                    count = len(data[0]) + 1
                else:
                    count = self.count + 2
                    
                self.schema=[('C'+str(i),'text') for i in xrange(1, count)]
        else:
            data = [[r] for r in data]

        yield self.schema

        for r in data:
            yield r

def Source():
    return vtbase.VTGenerator(clipboard)


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
