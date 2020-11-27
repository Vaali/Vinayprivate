from multiprocessing import Pool
import sys
import os
import glob
import loggingmodule
from itertools import repeat
from songsutils import CombineAlbums,resetZeroTagsFix
import shutil
import songs_api as api
import codecs

reload(sys)
sys.setdefaultencoding('utf8')

def movefiles1((src,dest)):
    fname = os.path.basename(src)
    destfname = os.path.join(dest, fname)
    if(not os.path.exists(destfname)):
        print "copying "+destfname
        shutil.move(src,dest)
    else:
            try:
                oldsong = api.parse(destfname)
                oldsong = resetZeroTagsFix(oldsong)
                mysong = api.parse(src)
                mysong = resetZeroTagsFix(mysong)
                print "checking "+destfname
                if(oldsong.isCompilation == True and mysong.isCompilation == False):
                    print "With this :"
                    mysong = CombineAlbums(oldsong,mysong)
                elif(round(oldsong.totalMatch) < round(mysong.totalMatch)):
                    print "With this :"
                    mysong = CombineAlbums(oldsong,mysong)
                    #print mysong.overLap
                elif ((round(oldsong.totalMatch) == round(mysong.totalMatch)) and (round(oldsong.songMatch) < round(mysong.songMatch))):
                    print "With this :"
                    mysong = CombineAlbums(oldsong,mysong)
                    #print mysong.match
                elif ((round(oldsong.songMatch) == round(mysong.songMatch)) and (round(oldsong.artistMatch) < round(mysong.artistMatch))):
                    
                    print "With this :"
                    mysong = CombineAlbums(oldsong,mysong)
                    #print mysong.totalMatch
                elif(round(oldsong.totalMatch) == round(mysong.totalMatch) and round(oldsong.songMatch) == round(mysong.songMatch) and round(oldsong.artistMatch) == round(mysong.artistMatch)):
                    if(mysong.releaseDate != 1001 and int(oldsong.releaseDate) > int(mysong.releaseDate)):
                        print "With this :"
                        mysong = CombineAlbums(oldsong,mysong)
                    elif(oldsong.decision == False):
                        mysong = CombineAlbums(oldsong,mysong)
                        mysong.releaseDate = oldsong.releaseDate
                    else:
                        mysong = oldsong    
                else:
                    mysong = oldsong
                fx = codecs.open(destfname,"w","utf-8")
                fx.write('<?xml version="1.0" ?>\n')
                mysong.export(fx,0)
                fx.close()    
            except Exception as ex:
                logger_matrix.exception(ex)
                logger_matrix.exception(fname)




if __name__ == '__main__':
    logger_matrix = loggingmodule.initialize_logger('copyxmls','copyxmls.log')
    directory = raw_input("Enter source directory: ")
    destination = raw_input("Enter destination directory: ")
    if(not os.path.exists(destination)):
		os.mkdir(destination)
    try:
        currdir = directory
        filelist = glob.glob(currdir+"/*.xml")
        p =Pool(processes=int(100))
        p.map(movefiles1,zip(filelist,repeat(destination)))
        p.close()
        p.join()
        for root,d_names,f_names in os.walk(directory):
            for d in d_names:
                currdir = os.path.join(root, d)
                filelist = glob.glob(currdir+"/*.xml")
                print filelist
                p =Pool(processes=int(100))
                p.map(movefiles1,zip(filelist,repeat(destination)))
                p.close()
                p.join()
    except Exception as e:
        logger_matrix.exception("Error")
