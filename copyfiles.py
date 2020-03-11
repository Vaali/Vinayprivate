from multiprocessing import Pool
import sys
import os
import glob
import loggingmodule
from itertools import repeat
import shutil


reload(sys)
sys.setdefaultencoding('utf8')

def movefiles1((src,dest)):
    fname = src[src.rfind('/')+1:]
    destfname = os.path.join(dest, fname)#dest+'/'+fname
    if(not os.path.exists(destfname)):
        print "copying "+destfname
        shutil.move(src,dest)


if __name__ == '__main__':
    logger_matrix = loggingmodule.initialize_logger('copyxmls','copyxmls.log')
    directory = raw_input("Enter source directory: ")
    destination = raw_input("Enter destination directory: ")
    try:
        filelist = glob.glob(directory+"/*.xml")
        p =Pool(processes=int(100))
        p.map(movefiles1,zip(filelist,repeat(destination)))
        p.close()
        p.join()
    except Exception as e:
        logger_matrix.exception("Error")
