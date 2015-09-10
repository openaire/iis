import sys, os
curmodulepath = os.path.dirname( os.path.abspath(__file__) )
sys.path.insert(0, os.path.abspath(os.path.join(curmodulepath,'..')))
sys.path.insert(0, os.path.abspath(os.path.join(curmodulepath,'..', 'lib')))
