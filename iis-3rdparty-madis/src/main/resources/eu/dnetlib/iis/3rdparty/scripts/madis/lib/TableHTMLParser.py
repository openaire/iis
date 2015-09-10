from HTMLParser import *
import re
import Queue


piencodingRegExp = '.*encoding=\"([^\"]+)\"'

metaencodingRegExp = '.*charset=([^\" ;]+)'

class TableHTMLParser(HTMLParser):
    "HTMLParser derived parser."

    bInspecting = True
    idle = 0
    intable = 1
    inheader = 2
    inraw = 3
    incolumn = 4
    state = idle
    tablesFound = 0

    def __init__(self, tableNum):
        "Initialise an object, passing 'verbose' to the superclass."
        HTMLParser.__init__(self)
        self.line = []
        self.lines = []
        self.header=[]
        self.value=''
        self.tableNum = tableNum
        self.encoding = 'utf-8'
        self.rowIndex = 0
        self.columnIndex = 0
        self.columnsNumber=0
        self.rowRepeats=dict()

    def close(self):
        self.f.close()


    def parse(self, s):
        "Parse the given string 's'."
        self.lines = []
        self.feed(unicode(s,self.encoding))
        if self.columnsNumber==0 and self.lines!=[]:
            self.columnsNumber=len(self.lines[0])
        for el in self.lines:
            lineSize=len(el)
            if lineSize<self.columnsNumber:
                el+=['']*(self.columnsNumber-lineSize)

            yield el
    def handle_pi(self,data):

        lst=re.findall(piencodingRegExp,data)
        if len(lst):
            self.encoding=lst[0]

    def handle_data(self, data):
        "Handle arbitrary data"
        if self.state == self.incolumn and self.bInspecting == False:
            self.value += data

    def handle_starttag(self, tag, attrs):
        dattrs=dict(attrs)
        if tag=='meta':
            if 'content' in dattrs:
                lst=re.findall(metaencodingRegExp,dattrs['content'])
                if len(lst):
                    self.encoding=lst[0]

        if tag == "table":
            self.tablesFound += 1
            self.state = self.intable
            if self.tablesFound == self.tableNum:
                self.bInspecting = False # table found
            else: self.bInspecting = True
        elif tag == "th":
            self.columnIndex+=1
            if 'colspan' in dattrs:
                self.replicatecolumn=int(dattrs['colspan'])
            else:
                self.replicatecolumn=0
            if 'rowspan' in dattrs:
                self.replicaterow+=int(dattrs['rowspan'])
            else:
                self.replicaterow=0

            self.state = self.incolumn
            self.value=''
            
        elif tag == "tr":
            self.rowIndex+=1
            self.line = [] # init line
            self.header = []
            self.state = self.inraw
            self.columnIndex=0
        elif tag == "td":           #### an exei colspan simeiwse to kai sto telos vale sti litsa ... an exei rowspan to idio an exei kai ta duo
            self.columnIndex+=1
            if 'colspan' in dattrs:
                self.replicatecolumn=int(dattrs['colspan'])
            else:
                self.replicatecolumn=0
            if 'rowspan' in dattrs:
                self.replicaterow+=int(dattrs['rowspan'])
            else:
                self.replicaterow=0
            self.value=''
            self.state = self.incolumn

    def handle_endtag(self, tag):
        if tag == 'table':
            self.state = self.idle
            self.bInspecting = True
        elif tag == "th":  ####
            if self.bInspecting == False:
                ####koita an proigeitai kati
                while self.rowIndex in self.rowRepeats and self.columnIndex in self.rowRepeats[self.rowIndex]:
                    self.header+=[self.rowRepeats[self.rowIndex][self.columnIndex]]
                    self.columnIndex+=1

                if self.replicatecolumn or self.replicaterow:
                    if self.replicatecolumn:self.replicatecolumn-=1
                    if self.replicaterow:self.replicaterow-=1
                    for i in range(self.replicaterow+1):
                        for j in range(self.replicatecolumn+1):
                            if i==j and i==0:
                                continue
                            curRow=i+self.rowIndex
                            if curRow not in self.rowRepeats:
                                self.rowRepeats[curRow]=dict()
                            self.rowRepeats[curRow][j+self.columnIndex]=self.value
                self.header+=[self.value]
            self.state = self.inraw
        elif tag == "tr" and self.bInspecting == False:
            self.columnIndex+=1
            if self.header!=[]:
                while self.rowIndex in self.rowRepeats and self.columnIndex in self.rowRepeats[self.rowIndex]:
                    self.header+=[self.rowRepeats[self.rowIndex][self.columnIndex]]
                    self.columnIndex+=1
            else:
                while self.rowIndex in self.rowRepeats and self.columnIndex in self.rowRepeats[self.rowIndex]:
                    self.line+=[self.rowRepeats[self.rowIndex][self.columnIndex]]
                    self.columnIndex+=1

            if self.header!=[]:
                self.lines+=[tuple(self.header)]
            else:
                self.lines+=[self.line]
            self.line=[]
            self.header=[]
            self.state = self.intable

            if self.rowIndex in self.rowRepeats:
                 del self.rowRepeats[self.rowIndex]
        elif tag == "td":       ### koitame an proigountai kapoia kopy ta vazoume kai colindex++
            if self.bInspecting == False:
                while self.rowIndex in self.rowRepeats and self.columnIndex in self.rowRepeats[self.rowIndex]:                    
                    self.line+=[self.rowRepeats[self.rowIndex][self.columnIndex]]
                    self.columnIndex+=1

                if self.replicatecolumn or self.replicaterow:
                    
                    if self.replicatecolumn:self.replicatecolumn-=1
                    if self.replicaterow:self.replicaterow-=1
                    for i in range(self.replicaterow+1):
                        for j in range(self.replicatecolumn+1):
                            if i==j and i==0:
                                continue
                            
                            curRow=i+self.rowIndex
                            if curRow not in self.rowRepeats:
                                self.rowRepeats[curRow]=dict()
                            self.rowRepeats[curRow][j+self.columnIndex]=self.value
                
                self.line+=[self.value]
                
            self.state = self.inraw
