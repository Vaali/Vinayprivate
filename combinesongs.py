import sys
import songs_api as api
import ConfigParser
import logging
import loggingmodule
import os
import glob
from multiprocessing import Pool


#config = ConfigParser.ConfigParser()

reload(sys)
sys.setdefaultencoding('utf8')
#mott the hoople:;227476:;blues:;1100:;18:;403915:;1969

def addsongs(fname):
    try:
        currsong = api.parse(fname)
        curr_string = currsong.songName 
        curr_string = curr_string + ':;' + currsong.youtubeId 
        curr_string = curr_string + ':;' + str(currsong.artistId) 
        if(currsong.artist.artistName[0] == None):
            return
        curr_string = curr_string + ':;' + currsong.artist.artistName[0]
        curr_string = curr_string + ':;' + str(currsong.viewcount)
        curr_string = curr_string + ':;' + str(currsong.earliestDate)

        genresList = currsong.level1Genres.genreName
        for genre in genresList:
            logger_genre.error(curr_string + ':;' + genre)
    
        styleList = currsong.level2Genres.genreName
        for style in styleList:
            logger_genre.error(curr_string + ':;' + style)
    except Exception as e:
        print fname
        print e


if __name__ == '__main__':
    if(not os.path.exists('logs')):
	os.mkdir('logs')
    else:
        try:
            os.remove('logs/combinedsongs.txt')
        except OSError:
            pass
    logger_genre = loggingmodule.initialize_logger('combinedsongs.txt',False)
    logger_errors = loggingmodule.initialize_logger1('combined_songserrors.log')
    directory = raw_input("Enter directory: ")
    foldlist = glob.glob(directory+"/*.xml")
    #addsongs('solr_newData11/0000F6q4PpfcPnY.xml')
    try:
        p =Pool(processes=int(10))
        p.map(addsongs,foldlist)
        p.close()
        p.join()
    except Exception as e:
        logger_errors.exception(e)



