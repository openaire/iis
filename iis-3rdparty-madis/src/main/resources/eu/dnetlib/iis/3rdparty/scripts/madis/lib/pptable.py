import cStringIO,operator

def indent(rows, hasHeader=False, headerChar='-', delim=' | ', justify='left',
           separateRows=False, prefix='', postfix='', wrapfunc=lambda x:x):
    """Indents a table by column.
       - rows: A sequence of sequences of items, one sequence per row.
       - hasHeader: True if the first row consists of the columns' names.
       - headerChar: Character to be used for the row separator line
         (if hasHeader==True or separateRows==True).
       - delim: The column delimiter.
       - justify: Determines how are data justified in their column. 
         Valid values are 'left','right' and 'center'.
       - separateRows: True if rows are to be separated by a line
         of 'headerChar's.
       - prefix: A string prepended to each printed row.
       - postfix: A string appended to each printed row.
       - wrapfunc: A function f(text) for wrapping text; each element in
         the table is first wrapped by this function."""

    logicalRows = [[row] for row in rows]
    # columns of physical rows
    columns = map(None,*reduce(operator.add,logicalRows))
    # get the maximum of each column by the string length of its items
    maxWidths = [max([len(str(item)) for item in column]) for column in columns]
    rowSeparator = headerChar * (len(prefix) + len(postfix) + sum(maxWidths) + \
                                 len(delim)*(len(maxWidths)-1))
    # select the appropriate justify method
    justify = {'center':str.center, 'right':str.rjust, 'left':str.ljust}[justify.lower()]
    output=cStringIO.StringIO()
    if separateRows: print >> output, rowSeparator
    for physicalRows in logicalRows:
        for row in physicalRows:
            print >> output, \
                (prefix \
                + delim.join([justify(str(item),width) for (item,width) in zip(row,maxWidths)]) \
                + postfix).rstrip()
        if separateRows or hasHeader: print >> output, rowSeparator; hasHeader=False
    return output.getvalue()

  
if __name__ == '__main__':
    labels = ('First Name', 'Last Name', 'Age', 'Position')
    data = \
    '''John,Smith,24,Software Engineer
       Mary,Brohowski,23,Sales Manager
       Aristidis,Papageorgopoulos,28,Senior Reseacher'''
    rows = [row.strip().split(',')  for row in data.splitlines()]

    print indent([labels]+rows, hasHeader=True)

    data=[['0asdf'],[1]]

    print indent(data, hasHeader=True)
    
    # output:
    #
    #Without wrapping function
    #
    #First Name | Last Name        | Age | Position         
    #-------------------------------------------------------
    #John       | Smith            | 24  | Software Engineer
    #Mary       | Brohowski        | 23  | Sales Manager    
    #Aristidis  | Papageorgopoulos | 28  | Senior Reseacher 
    #
