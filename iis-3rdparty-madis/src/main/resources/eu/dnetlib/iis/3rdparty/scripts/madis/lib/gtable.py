from lib.simpleutils import latinnum

from lib.unicodeops import unistr

def numeric(el): ####Oi upoloipoi typoi
    el=unistr(el)
    if el.startswith("0") and not el.startswith("0."):
        return el
    try:
        return int(el)
    except ValueError:
        try:            
            return float(el)
        except ValueError:
            return el


def gjsonFull(rows,titleslist,typelist): ###does not support date type

    import json
    response={"cols":[],"rows":[]}    
    for name, type,num in zip(titleslist, typelist,xrange(len(titleslist))):
        id=latinnum(num+1)
        response["cols"]+=[{"id":id, "label":name,"type":type}]
    for row in rows:
        rowdict={"c":[]}
        for val,type in zip(row, typelist):
            if type=="number":
                try:
                    rowdict["c"]+=[{"v":numeric(val)}]
                except ValueError:
                    raise ValueError("Type problem in %s:\nRow:%sTypes:%s" %(val,row,typelist))
            else:
                rowdict["c"]+=[{"v":val}]
        response["rows"]+=[rowdict]
    
    return json.dumps(response,sort_keys=True)

def mkoutputGoogleTableCol(names, types):
    #######{id:'A',label:'State',type:'string'}
    counter = ord('A')
    rawValues = ""
    for name, type in zip(names, types):
        rawValues += "{id:'%s',label:'%s',type:'%s'},"  % (chr(counter), name, type)
        counter += 1
    return rawValues[:-1] ####OMITT THE LAST COMMA


def mkoutputGoogleTableRow(values, types, format = '%Y-%m'):
    #####        {c:[{v:'Alabama'},{v:Date(1,1,2008),f:'Jan-08'},{v:203.6,f:'203.6'},{v:4.5,f:'4.5'},{v:'South'},{v:4452.0,f:'4452'},{v:0.122,f:'0.122'}]}
    #print values
    rawValues = ""
    for val, type in zip(values, types):
        if type == 'string':
            if val == 'null':
                rawValues += "{v:%s}," % (val)
            else:
                rawValues += "{v:'%s'}," % (val)
        elif type == 'date':
            timemonth = time.strptime(val, format)
            rawValues += "{v:new Date(%s,%s,1)}," % (int(time.strftime('%Y', timemonth)), int(time.strftime('%m', timemonth))-1)
        else: #type =='number'
            if val == 'null':
                rawValues += "{v:0},"
            else:
                rawValues += "{v:%s}," % (val)

    rawRow = "{c:[%s]}" % (rawValues[:-1]) ####OMITT THE LAST COMMA
    return rawRow

def gtableIter(iter,names,types):
    yield '{cols:[%s],rows:[' %(mkoutputGoogleTableCol(names, types))
    f=True
    for row in iter:
        if f:
            f=False
            yield mkoutputGoogleTableRow(row, types)
        else:
            yield ","+mkoutputGoogleTableRow(row, types)
    yield ']}'
    return

def gjsonIter(rows,titleslist,typelist): ###does not support date type

    import json
    #response={"cols":[],"rows":[]}
    yield '{"cols": '
    header=[]
    first=True
    for name, type,num in zip(titleslist, typelist,xrange(len(titleslist))):
        id=latinnum(num+1)
        header+=[{"id":id, "label":name,"type":type}]
    yield json.dumps(header,sort_keys=True)
    yield ', "rows": ['
    for row in rows:
        rowdict={"c":[]}
        for val,type in zip(row, typelist):
            if type=="number":
                try:
                    rowdict["c"]+=[{"v":numeric(val)}]
                except ValueError:
                    raise ValueError("Type problem in %s:\nRow:%sTypes:%s" %(val,row,typelist))
            else:
                rowdict["c"]+=[{"v":val}]
        if not first:
            yield ', '+json.dumps(rowdict,sort_keys=True)
        else:
             yield json.dumps(rowdict,sort_keys=True)
        first=False
    yield ']}'
    
    return 

#def gtableFull(rows,titleslist,typelist): ORIGINAL
#    columsstr = mkoutputGoogleTableCol(titleslist, typelist)
#    googleDatatable = [mkoutputGoogleTableRow(row, typelist, dateformat) for row in rows]
#    rowstr = ','.join(googleDatatable)
#    googleDatatableJSstring = "{cols:[%s],rows:[%s]}" % (columsstr, rowstr)
#    return googleDatatableJSstring

def gtableFull(rows,titleslist,typelist):
    return  "".join(gtableIter(rows,titleslist,typelist))




def gjsonfileFull(iterin,fiterout,names,types):
    for wr in gjsonIter(iterin, names, types):
        fiterout.write(wr.encode('utf-8'))
def gtablefileFull(iterin,fiterout,names,types):
    for wr in gtableIter(iterin, names, types):
        fiterout.write(wr.encode('utf-8'))