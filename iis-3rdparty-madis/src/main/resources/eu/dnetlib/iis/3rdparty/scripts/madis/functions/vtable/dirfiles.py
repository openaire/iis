"""
.. function:: dirfiles([rec:1], directory_name) -> path_filename, filename

Returns the files name in a given directory. With the option 'rec:1' it returns
the files under the provided directory and all its subdirectories.

This function is very usefull when used with the *execprogram* function to execute
an external command for every filename.

.. note::
    *Dirfiles* does not follow links.

:Returned table schema:
    Column C1 is the full filename (path/filename)
    Column C2 is filename

Examples:

    >>> sql("select c2 from dirfiles('.') where c2 like 'f%.py'")    # doctest:+ELLIPSIS +NORMALIZE_WHITESPACE
    c2
    -------
    file.py
    flow.py

    >>> sql("select c2 from dirfiles('rec:1','.') where c2 like 'c%.py'")    # doctest:+ELLIPSIS +NORMALIZE_WHITESPACE
    c2
    ------------
    coltypes.py
    clipout.py
    cache.py
    continue.py
    clipboard.py

"""
import vtbase
import os.path
import functions
import os

registered=True

class dirfiles(vtbase.VT):
    def VTiter(self, *parsedArgs, **envars):
        def expandedpath(p):
            return os.path.realpath(os.path.abspath(os.path.expanduser(os.path.expandvars(os.path.normcase(os.path.normpath(p))))))

        yield [('c1', 'text'), ('c2', 'text')]

        opts = self.full_parse(parsedArgs)

        dirname='.'
        recursive=False

        if 'rec' in opts[1]:
            del opts[1]['rec']
            recursive=True

        if 'r' in opts[1]:
            del opts[1]['r']
            recursive=True

        if not recursive and len(opts[0])+len(opts[1])>1:
            if opts[0][0]=='rec' or opts[0][0]=='recursive':
                recursive=True
                del opts[0][0]

        if 'query' in opts[1]:
            dirname=query
        elif len(opts[0])>0:
            dirname=opts[0][-1]
        elif len(opts[0])==len(opts[1])==0:
            dirname='.'
        else:
            functions.OperatorError(__name__.rsplit('.')[-1], 'A directory name should be provided')

        dirname=expandedpath(dirname)

        if not recursive:
            for f in os.listdir(dirname):
                fullpathf=expandedpath(os.path.join(dirname,f))
                if os.path.isfile(fullpathf):
                    yield (fullpathf, f)
        else:
            for root, dirs, files in os.walk(dirname):
                for f in files:
                    fullpathf=expandedpath(os.path.join(root, f))
                    if os.path.isfile(fullpathf):
                        yield (fullpathf, f)

def Source():
    return vtbase.VTGenerator(dirfiles)


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
