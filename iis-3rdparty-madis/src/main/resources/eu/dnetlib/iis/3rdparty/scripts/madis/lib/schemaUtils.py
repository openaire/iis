import re

reduce_spaces=re.compile(ur'\s+', re.UNICODE)

def CreateStatement(description,tablename):
    names=[]
    types=[]
    for tp in description:
        names+=[tp[0]]
        if len(tp)==1:
            types+=['None']
        else:
            if tp[1]!=None:
                types+=[tp[1]]
            else:
                types+=['None']
    return schemastr(tablename,names,types)

def unify(slist):
    if len(set(slist))==len(slist):
        return slist
    eldict={}
    for s in slist:
        if s in eldict:
            eldict[s]+=1
        else:
            eldict[s]=1
    for val,fr in eldict.items():
        if fr==1:
            del eldict[val]
    for val in eldict:
        eldict[val]=1
    uniquelist=[]
    for s in slist:
        if s in eldict:
            uniquelist+=[s+str(eldict[s])]
            eldict[s]+=1
        else:
            uniquelist+=[s]

    return uniquelist

onlyalphnum=re.compile('[a-zA-Z]\w*$')

def schemastr(tablename,colnames,typenames=None):
    stripedcolnames=['"'+el+'"' if onlyalphnum.match(el)
                                else '"'+reduce_spaces.sub(' ', el.replace('\n','').replace('\t','')).strip().replace('"','""')+'"'
                                for el in unify(colnames)]
    if not typenames:
        return "create table %s(%s)" %(tablename,','.join([c for c in stripedcolnames]))
    else:
        stripedtypenames=['' if el.lower()=="none" or el==''
                            else el if onlyalphnum.match(el)
                            else '"'+el.replace('"','""')+'"'
                            for el in typenames]
        return "create table %s(%s)" %(tablename,','.join([c+' '+str(t) for c,t in zip(stripedcolnames,stripedtypenames)]))
