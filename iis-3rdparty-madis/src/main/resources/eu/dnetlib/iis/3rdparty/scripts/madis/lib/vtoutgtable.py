from gtable import gtablefileFull , gjsonfileFull ,gjsonFull
from iterutils import peekable
from sqlitetypes import typestoSqliteTypes
import types


def vtoutpugtformat(out,diter,simplejson=True): #### TODO Work on types patttern
    """
    Reads diter stream of tuples(row,types) and formats row values to
    the google json format or if simplejson is False to the google like format.
    Writes formated tables in file like stream out the
    """
    def unfold(it):        
        for row,h in it:
            yield row
        return    
    d=peekable(diter)
    samplevals, sampleheads =d.peek()
    names=[]
    gtypes=[]
    
    mustguess=False
    for val, headinfo in zip(samplevals, sampleheads):
        names.append(headinfo[0].title())
        coltype=typestoSqliteTypes(headinfo[1])
        
        if coltype=="INTEGER" or coltype=="REAL" or coltype=="NUMERIC":
            gtypes.append('number')
        elif coltype=="TEXT":
            gtypes.append('string')
        else:
            mustguess=True

            gtypes.append("GUESS")
    if mustguess:
        samples=d.maxpeek(30)
        samplestats=dict()
        for i in xrange(len(gtypes)):
            if gtypes[i]=="GUESS":
                samplestats[i]={'string':False,"number":False}
        for row in unfold(samples):
            allknown=True
            for uto in samplestats:
                if not samplestats[uto]['string']:
                    allknown=False
                    if row[uto]!="":
                        samplestats[uto][typeguessing(row[uto])]=True
            if allknown:
                break
        for uto in samplestats:
            if samplestats[uto]['string']:# or not samplestats[uto]['number']:
                gtypes[uto]='string'
            else:
                gtypes[uto]='number'


    if simplejson:
        #out.write(gjsonFull(unfold(d),names,gtypes).encode('utf-8'))
        gjsonfileFull(unfold(d),out,names,gtypes)
    else:
        gtablefileFull(unfold(d),out,names,gtypes)
        #out.write(gtableFull(unfold(d),names,gtypes).encode('utf-8'))
    


def typeguessing(el): ####Oi upoloipoi typoi
#    import types
#    if type(el) not in types.StringTypes:
#        print "Element is : --%s-- , Type is %s Type of element not string!!!!!!!!!!!!!" %(el,type(el))
#        raise Exception
    if type(el) not in types.StringTypes:
        el=str(el)
    if el.startswith("0") and not el.startswith("0."):
        return 'string'
    try:
        int(el)            
        return 'number'
    except ValueError:
        try:
            float(el)            
            return 'number'
        except ValueError:
            return 'string'



"""
cols property
---------------
cols is an array of objects describing the ID and type of each column. Each property is an object with the following properties (case-sensitive):

    * type [Required] Data type of the data in the column. Supports the following string values (examples include the v: property, described later):
          o 'boolean' - JavaScript boolean value ('true' or 'false'). Example value: v:'true'
          o 'number' - JavaScript number value. Example values: v:7 , v:3.14, v:-55
          o 'string' - JavaScript string value. Example value: v:'hello'
          o 'date' - JavaScript Date object (zero-based month), with the time truncated. Example value: v:new Date(2008, 0, 15)
          o 'datetime' - JavaScript Date object including the time. Example value: v:new Date(2008, 0, 15, 14, 30, 45)
          o 'timeofday' - Array of three numbers and an optional fourth, representing hour (0 indicates midnight), minute, second, and optional millisecond. Example values: v:[8, 15, 0], v: [6, 12, 1, 144]
    * id [Optional] String ID of the column. Must be unique in the table. Use basic alphanumeric characters, so the host page does not require fancy escapes to access the column in JavaScript. Be careful not to choose a JavaScript keyword. Example: id:'col_1'
    * label [Optional] String value that some visualizations display for this column. Example: label:'Height'
    * pattern [Optional] String pattern that was used by a data source to format numeric, date, or time column values. This is for reference only; you probably won't need to read the pattern, and it isn't required to exist. The Google Visualization client does not use this value (it reads the cell's formatted value). If the DataTable has come from a data source in response to a query with a format clause, the pattern you specified in that clause will probably be returned in this value. The recommended pattern standards are the ICU DecimalFormat and SimpleDateFormat.
    * p [Optional] An object that is a map of custom values applied to the cell. These values can be of any JavaScript type. If your visualization supports any cell-level properties, it will describe them; otherwise, this property will be ignored. Example: p:{style: 'border: 1px solid green;'}.

"""