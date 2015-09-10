import re
from unicodeops import unistr


def parse(args,boolargs=None,nonstringargs=None,needsescape=None,notsplit=None):
    if boolargs==None:
        boolargs=[]
    if nonstringargs==None:
        nonstringargs=dict()
    if needsescape==None:
        needsescape=[]
    if notsplit==None:
        notsplit=[]

    listargs, keyargs= parametrize(*[unquote(unistr(a)) for a in args],**{'escapelists':needsescape,'notsplit':notsplit})
    keyargsdict=translate(keyargs,boolargs,nonstringargs)
    return listargs, keyargsdict


def unescape(arg):
    arg=unistr(arg)
    q=arg.split("'")
    qlist=[]
    for qi in q:
        l=qi.split('\n')
        llist=[]
        for li in l:
            if li.endswith('\\'):
                llist+=[eval("'%s'" %(li.replace('\\','\\\\'))).replace('\\\\','\\') ]
            else:
                llist+=[eval("'%s'" %(li)) ]
        qlist+=['\n'.join(llist)]
    return "'".join(qlist)


def translate(dictargs,boolargs,nonstringargs):
    for key in dictargs:
        if key in boolargs:
            val = dictargs[key].lower()
            if val!='f' and val!='false' and val!='0':
                dictargs[key]=True
            else:
                dictargs[key]=False
        elif key in nonstringargs:
            val=dictargs[key]
            if dictargs[key] in nonstringargs[key]:
                dictargs[key]=nonstringargs[key][dictargs[key]]
            else:
                raise Exception("Argument parsing: Not valid value for argument '%s' " %(key))
    return dictargs


def unquote(p):
    if p.startswith("'") and p.endswith("'"):
        return p[1:-1].replace("''","'")
    elif p.startswith('"') and p.endswith('"'):
        return p[1:-1].replace('""','"')
    return p


re_params=re.compile(ur'^(?!\w:\\\w)(\w+):(.*)')

def parametrize(*args,**kargs):
    ps=[]
    kps=dict()
    escapelists=[]
    if 'escapelists' in kargs:
        escapelists=kargs['escapelists']

    for p in args:        
        splitable=re_params.match(p)
        if not splitable:
            ps.append(p)
        else:
            if splitable.groups()[0] in kargs['notsplit']:
                ps.append(p)
                continue
            if splitable.groups()[0] in escapelists:
                kps[str(splitable.groups()[0])]=unescape(splitable.groups()[1])
            else:
                kps[str(splitable.groups()[0])]=splitable.groups()[1]
    return ps,kps


