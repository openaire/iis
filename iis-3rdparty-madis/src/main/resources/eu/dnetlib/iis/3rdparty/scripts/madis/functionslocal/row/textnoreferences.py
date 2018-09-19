# coding: utf-8

import re
import json


def textnoreferences(txt,maxlen = 5,pattern = r'(\b|_)((1[5-9]\d{2,2})|(20\d{2,2}))(\b|_)' ):
    """
    .. function:: textreferences(text, maxlen = 5, pattern = (\b|_)(1|2)\d{3,3}(\b|_))

    Returns whole documents without the "Reference" section of documents. To find it, it searches for parts of the document that
    have a high density of pattern matches.

    .. parameters:: txt,maxlen,pattern
       txt: input text.
       maxlen: the size of the scrolling window over the text in which the density is calculated.
       pattern: regular expression that is matched against the lines of the text. By default the pattern matches
                year occurences so as to extract sections that look like references.


    Examples:

    >>> table1('''
    ... eeeeeeeeeeeeee
    ... gggggggggggggg
    ... aaaaaaaaaaaaaa
    ... bbbbbbbbbbbbbb
    ... aaa_1914_ccccc
    ... bbb_2014_bbbbb
    ... dddd_2008_ddddddd
    ... cccc_2005_ccccc
    ... ccccc_2014_ccccc
    ... dddddd_2009_ddddd
    ... gggggggggggggg
    ... ''')

    >>> sql("select textnoreferences(group_concat(a,'\\n'),1,'(\b|_)(1|2)\d{3,3}(\b|_)') as a from table1")
    a
    --------------------------------------------------------------------------------------------------
    aaa_1914_ccccc
    bbb_2014_bbbbb
    dddd_2008_ddddddd
    cccc_2005_ccccc
    ccccc_2014_ccccc
    dddddd_2009_ddddd


    If an inadequate amount of newlines is found, it returns the text as is.

    >>> sql("select textnoreferences(group_concat(a,'.')) from table1")
    textreferences(group_concat(a,'.'))
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    eeeeeeeeeeeeee.gggggggggggggg.aaaaaaaaaaaaaa.bbbbbbbbbbbbbb.aaa_1914_ccccc.bbb_2014_bbbbb.dddd_2008_ddddddd.cccc_2005_ccccc.ccccc_2014_ccccc.dddddd_2009_ddddd.gggggggggggggg


    >>> sql("select textnoreferences('')")
    textreferences('')
    ------------------
    <BLANKLINE>
    """

    exp = re.sub('\r\n','\n',txt)

    if exp.count('\n')<10:
        return exp
    noreferences = []
    reversedtext = iter(reversed(exp.split('\n')[10:]))
    reversedtext2 = iter(reversed(exp.split('\n')[10:]))
    results = []
    densities = []

    for i in xrange(maxlen/2):
        results.append(1)
    for i in reversedtext:
        if len(i)>10:
            if re.search(pattern,i):
                    results.append(1)
            else:
                    results.append(0)

    for i in xrange(maxlen/2):
        results.append(0)

    out = 0
    temp = 0
    for i in xrange(maxlen/2,len(results)-maxlen/2):
        if i==maxlen/2 :
            temp = sum(results[0:maxlen])*1.0/maxlen
        else:
            if out == results[i+maxlen/2]:
                pass
            elif results[i+maxlen/2]:
                temp = (temp*maxlen+1) *1.0 / maxlen
            else:
                temp = (temp*maxlen-1) *1.0 / maxlen
        densities.append(temp)
        out = results[i-maxlen/2]

    try:
        threshold =  sum(densities)/len(densities)
    except:
        threshold = 0

    current = 0
    for i in reversedtext2:
        if len(i)>10:
            if densities[current] < threshold:
                noreferences.append(i)
            current+=1
        else:
            noreferences.append(i)
    return  '\n'.join(reversed(noreferences))

textnoreferences.registered=True


if not ('.' in __name__):
    """
    This is needed to be able to test the function, put it at the end of every
    new function you create
    """
    import sys
    import src.functions.row.setpath
    from functions import *
    testfunction()
    if __name__ == "__main__":
        reload(sys)
        sys.setdefaultencoding('utf-8')
        import doctest
        doctest.testmod()
