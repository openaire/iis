"""
This is the jgroup module

It features conversion to and from jlists

>>> toj(3)
3
>>> toj('3')
'3'
>>> toj('test')
'test'
>>> toj(u'test')
u'test'
>>> toj('[testjsonlike]')
'["[testjsonlike]"]'
>>> toj('[testjsonlike')
'[testjsonlike'
>>> toj([3])
3
>>> toj(['test'])
'test'
>>> toj(['test',3])
'["test",3]'
>>> toj([3,'test'])
'[3,"test"]'
>>> toj(['[test'])
'[test'
>>> toj(None)

>>> toj('')
u''
>>> toj([])
u'[]'
>>> tojstrict('asdf')
'["asdf"]'
>>> tojstrict(['a',3])
'["a",3]'
>>> fromj('["a", 3]')
[u'a', 3]
>>> fromj(3)
[3]
>>> fromj('a')
['a']
>>> fromj('["a", 3]')
[u'a', 3]
>>> fromj('[null]')
[None]
>>> fromj('[asdf]')
['[asdf]']
>>> fromj('')
[u'']
>>> fromj('[]')
[]
>>> elemfromj(1,2,3)
[1, 2, 3]
>>> elemfromj(1,None,3)
[1, None, 3]
>>> fromjsingle("[1,2]")
[1, 2]
"""

import json
try:
    from collections import OrderedDict
except ImportError:
    # Python 2.6
    from lib.collections26 import OrderedDict

def toj(l):
    if l==None:
        return l
    typel=type(l)
    if typel==str or typel==str:
        if l=='':
            return ''
        elif l[0]!='[' or l[-1]!=']':
            return l
        else:
            return json.dumps([l], separators=(',',':'), ensure_ascii=False)
    if typel==int or typel==float:
        return l
    if typel==list or typel==tuple:
        lenl=len(l)
        if lenl==1:
            typel=type(l[0])
            if typel==str or typel==str:
                if l[0]=='':
                    return ''
                elif  l[0][0]!='[' or l[0][-1]!=']':
                    return l[0]
            if typel==int or typel==float:
                return l[0]
        if lenl==0:
            return '[]'
        return json.dumps(l, separators=(',',':'), ensure_ascii=False)
    return json.dumps(l, separators=(',',':'), ensure_ascii=False)

def tojstrict(l):
    if type(l)==list:
        return json.dumps(l, separators=(',',':'), ensure_ascii=False)
    return json.dumps([l], separators=(',',':'), ensure_ascii=False)

def fromjsingle(j):
    typej=type(j)
    if typej == int or typej == float:
        return j
    if typej == str or typej == str:
        if j == '':
            return ''
        if (j[0] == '[' and j[-1] == ']') or (j[0]=='{' and j[-1]=='}'):
            try:
                return json.loads(j, object_pairs_hook = OrderedDict)
            except KeyboardInterrupt:
                raise
            except:
                return j
        return j

def fromj(*jargs):
    fj=[]
    for j in jargs:
        typej=type(j)
        if typej==int or typej==float:
            fj+= [j]
            continue
        if typej==str or typej==str:
            if j=='':
                fj+= ['']
                continue
            if (j[0]=='[' and j[-1]==']'):
                try:
                    fj+=json.loads(j)
                    continue
                except KeyboardInterrupt:
                    raise
                except:
                    fj+= [j]
                    continue
            if (j[0]=='{' and j[-1]=='}'):
                try:
                    fj+=list(json.loads(j, object_pairs_hook = OrderedDict))
                    continue
                except KeyboardInterrupt:
                    raise
                except:
                    fj+= [j]
                    continue
            fj+= [j]
    return fj

def elemfromj(*jargs):
    fj=[]
    for j in jargs:
        if j is None:
            fj+=[None]
            continue
        typej=type(j)
        if typej==int or typej==float:
            fj+= [j]
            continue
        if typej==str or typej==str:
            if j=='':
                fj+= ['']
                continue
            if j[0]=='[' and j[-1]==']':
                try:
                    fj+=[json.loads(j)]
                    continue
                except KeyboardInterrupt:
                    raise
                except:
                    fj+= [j]
                    continue
            if (j[0]=='{' and j[-1]=='}'):
                try:
                    fj+=[json.loads(j, object_pairs_hook = OrderedDict)]
                    continue
                except KeyboardInterrupt:
                    raise
                except:
                    fj+= [j]
                    continue          
            fj+= [j]
    return fj

#Flatten based on BasicTypes for Python
#	Copyright (c) 2002-2003, Michael C. Fletcher
#	All rights reserved.
def flatten(inlist, type=type, ltype=(list,tuple)):
    try:
        ind=0
        while True:
            while isinstance( inlist[ind], ltype):
                inlist[ind:ind+1] = list(inlist[ind])
            ind+=1
    except IndexError:
        pass
    return inlist

if __name__ == "__main__":
    import doctest
    doctest.testmod()
