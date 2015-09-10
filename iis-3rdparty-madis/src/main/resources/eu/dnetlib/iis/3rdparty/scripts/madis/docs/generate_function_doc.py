import sys, os
sys.path.append((os.path.join(sys.path[0],'..')))

import functions
from shutil import copyfile

functions.register()

curpath = os.path.abspath(sys.path[0])

def gendoc(funtype, toplevelonly=False):
    file=open(os.path.join(curpath, 'source', funtype+'.txt'),'w')

    tmpstr=".. _"+(funtype.lower()+' functions list:').replace(' ','-')+'\n\n'
    tmpstr+=funtype[0].upper()+funtype[1:]+' functions list'
    file.write(tmpstr+'\n')
    file.write('='*len(tmpstr)+'\n\n')
    file.write('.. automodule:: functions.'+funtype+'.__init__\n\n')

    docs={}

    if not toplevelonly:
        for i,v in functions.functions[funtype].iteritems():
            if not v.__name__.startswith('functionslocal'):
                if v.__module__ not in docs:
                    docs[v.__module__]=[]
                docs[v.__module__].append(i)
    else:
        for i,v in functions.functions[funtype].iteritems():
            if not v.__name__.startswith('functionslocal'):
                tstr='functions.vtable.'+i
                if tstr not in docs:
                    docs[tstr]=True

    for i,v in sorted(docs.items()):
        modulestr='.. module:: functions.'+funtype+'.'+i.split('.')[2]+'\n\n'
        
        if not toplevelonly:
            modstr=':mod:`'+i.split('.')[2]+'` functions'
        else:
            modstr=':mod:`'+i.split('.')[2]+'` function'
        file.write(modstr+'\n')
        file.write('-'*len(modstr)+'\n\n')
        file.write(modulestr)
        file.write('')
        
        if not toplevelonly:
            for i1 in sorted(v):

                tmpstr='**'+i1+' function**'
                file.write(tmpstr)
                if not functions.functions[funtype][i1].__doc__:
                    file.write('\n\n')
                else:
                    file.write(str(functions.functions[funtype][i1].__doc__)+'\n\n')
        else:
            if not functions.functions[funtype][i.split('.')[-1]].__doc__:
                file.write('\n\n')
            else:
                file.write(str(functions.functions[funtype][i.split('.')[-1]].__doc__)+'\n\n')
                
copyfile(os.path.join(curpath, '..', 'LICENSE.txt'), os.path.join(curpath, 'source', 'license.txt'))
gendoc('row')
gendoc('aggregate')
gendoc('vtable', True)
