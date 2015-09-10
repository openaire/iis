import zipfile 
import tempfile
class ZipIter:
    def __init__(self,filename,mode="r"):
        if mode!="r" and mode!="rb" and mode!="w" and mode!="wb":
            raise Exception("Unknown mode")
        self.mode=mode
        if mode[0]=="r": #READ MODE
            self.zipfolder=zipfile.ZipFile(filename,mode)
            self.name=self.zipfolder.namelist()[0]
            self.zipfolder.extract(self.name)
            self.zipfolder.close()
            self.f=open(self.name,mode)

            pass
        if mode[0]=="w": #WRITE MODE
            self.zipfolder=zipfile.ZipFile(filename+".zip",mode)
            self.name=filename
            self.f=open(filename,mode)
    def __iter__(self):
        return self
    def __getattr__(self, attr):
        if self.__dict__.has_key(attr):
            return self.__dict__[attr]
        return getattr(self.f, attr)

    

    def close(self):
        self.f.close()
        if self.mode[0]=="w":
            self.zipfolder.write(self.name,self.name.rsplit('/',1)[-1])
            self.zipfolder.close()
        import os
        os.remove(self.name)