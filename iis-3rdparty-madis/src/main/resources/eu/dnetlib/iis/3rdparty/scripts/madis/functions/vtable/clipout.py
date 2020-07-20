"""
.. function:: clipout([h:0], query:None)

Writes to clipboard the output of *query*. The clipboard contents will be tab delimited.

:header option:

    if an 'h' or h:1 option is found then it also exports the schema of the query.

:Returned table schema:
    - *return_value* int
        Boolean value 1 indicating success. On failure an exception is thrown.

Examples:

    >>> sql("clipout select 5,6")
    return_value
    ------------
    1
"""

import setpath
from vtout import SourceNtoOne
import os
import functions

registered=True

def Clipout(diter, schema, *args, **kargs):
    import lib.pyperclip as clip
    a=[]

    exportheader=False

    for i in args:
        if i.startswith('h'):
            exportheader=True

    for i in kargs:
        if i.startswith('h'):
            exportheader=True

    if exportheader==True:
        a.append(u'\t'.join([unicode(i[0]).replace('\t','    ').replace('\n',' ') for i in schema]).encode('utf_8', 'replace'))
        exportheader=False

    for row in diter:
        a.append(u'\t'.join([unicode(i).replace('\t','    ').replace('\n',' ') for i in row]).encode('utf_8', 'replace'))

    if os.name == 'nt':
        clip.setcb(functions.mstr('\n'.join(a)))
    else:
        clip.setcb('\n'.join(a))

def Source():
    return SourceNtoOne(Clipout)

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