"""
One to N operator continue
This functions executes a query that is given as a parameter and returns 1
if the execution succeds or 0 if the execution fails

    >>> #sql("continue select 5")
    return_value
    ------------
    1
    >>> #sql("continue lalakis")
    return_value
    ------------
    0
"""

import setpath          #for importing from project root directory  KEEP IT IN FIRST LINE
from vtout import SourceNtoOne
import functions

 #UNCOMMENT TO REGISTER THE N to 1 OPERATOR
registered=False

def execontinue(diter, schema, *args,**kargs):
    if args or kargs:
        raise functions.OperatorError(__name__.rsplit('.')[-1],"operator takes no arguments")
    
    for el in diter:
        pass    
    pass

def Source():

    return SourceNtoOne(execontinue,retalways=True)

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