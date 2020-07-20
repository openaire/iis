# coding: utf-8

import setpath
import datetime
from lib import porter2 as porter
from lib.textcat import *

classifier=NGram()

def detectlang(*args):

    """
    .. function:: detectlang(text1, text2,...) -> text

    Detects the language of a snippet of text by analysing its statistical properties.

    Examples:

    >>> sql("detectlang 'ελληνικά'")
    detectlang('ελληνικά')
    ------------------------------
    greek-utf
    >>> sql("detectlang this is in english")
    detectlang('this is in english')
    --------------------------------
    english
    >>> sql("detectlang ceci est en français")
    detectlang('ceci est en français')
    -----------------------------------
    french
    >>> sql("detectlang este es el español")
    detectlang('este es el español')
    ---------------------------------
    spanish
    """

    if len(args)==0:
        return

    l=''.join(args).encode('utf-8')

    if l=='':
        return

    return classifier.classify(l)

detectlang.registered=True

def stem(*args):

    """
    .. function:: stem(text1, text2,...) -> text

    Does stemming according to the porter algorithm.

    Examples:

    >>> sql("stem 'cutting and creating'")
    stem('cutting and creating')
    ----------------------------
    cut and creat

    >>> sql("stem ceci est en français cutting")
    stem('ceci est en français cutting')
    -------------------------------------
    ceci est en françai cut

    """

    out=[]
    for i in args:
        o=i.lower()
        o=o.strip()
        o=o.split(' ')

        for k in o:
            if len(k)>0:
                out.append(porter.stem(k))

    return ' '.join(out)

stem.registered=True

def stem_en(*args):

    """
    .. function:: stem_en(text1, text2,...) -> text

    Detects if the input is in english and only then does the porter stemming else
    it returns the input arguments concatenated

    Examples:

    >>> sql("stem_en 'cutting and creating'")
    stem_en('cutting and creating')
    -------------------------------
    cut and creat

    >>> sql("stem_en ceci est en français cutting")
    stem_en('ceci est en français cutting')
    ----------------------------------------
    ceci est en français cutting

    """

    jargs=''.join(args)

    if detectlang(*args)!='english':
        return jargs

    out=[]
    for i in args:
        o=i.lower()
        o=o.strip()
        o=o.split(' ')

        for k in o:
            if len(k)>0:
                out.append(porter.stem(k))

    return ' '.join(out)

stem_en.registered=True


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
