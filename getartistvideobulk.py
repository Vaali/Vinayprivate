import sys
from multiprocessing import Pool
import managekeys
import loggingmodule
from datetime import datetime, date, timedelta


reload(sys)
sys.setdefaultencoding('utf8')


logger_error = loggingmodule.initialize_logger('errors_inbulk','errors_inbulk.log')


def runYoutubeApi(directory):
    try:
        #print fl
        start_time = datetime.now()
        logger_decisions.error(directory + " ---- runYoutubeApi started ---")
        curr_artist_dir = os.path.basename(directory)
        search_url = "https://www.googleapis.com/youtube/v3/search?part=snippet&q="+urllib.quote_plus(str(allArtists))+"+"+urllib.quote_plus(str(video.name))+"&alt=json&type=video&maxResults=50&key="+key+"&videoCategoryId=10"
                
        print 'Total length of items'
        print len(list(return_search_results['items']))
        #with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        #        return_pool = executor.map(getVideoFromYoutube,zip(parallel_songs_list,repeat(return_search_results)))
        
        with open(lastrunfile, 'wb') as f2:
            print 'dumping '+str(int(time.time()))
            pickle.dump(str(int(time.time())),f2)
        logger_decisions.error(directory + " -- runYoutubeApi Completed with time -- " + str(datetime.now() - start_time))
    except Exception as e:
        print e
        logger_error.exception(e)
 

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf8')
    filenameList = []
    t1 = datetime.now()
    manager = managekeys.ManageKeys()
    manager.reset_projkeys()
    
    try:
        lastdirectory = 0
        logger_error.debug("Discogs Main Program Starting")
        directory = raw_input("Enter directory: ")
        m1 = raw_input("Enter m: ")
        folders = raw_input("Enter number of folders: ")
        folders = int(folders)
        m1=int(m1)
        timeFile = directory + "/timelog.txt"
        if(os.path.exists(directory+'/lastdirectory.txt')):
            fread = codecs.open(directory+'/lastdirectory.txt','r','utf-8')
            lines = fread.readlines()
            if(len(lines) > 0):
                if(lines[-1].strip() != ""):
                    lastdirectory = int(lines[-1])
            fread.close()
            fwrite = codecs.open(directory+'/lastdirectory.txt','a','utf-8')
        else:
            fwrite = codecs.open(directory+'/lastdirectory.txt','w','utf-8')

        ''' Folders list count to control the numebr of folders done'''
        for dirs in os.listdir(directory):
            found = re.search(r'[0-9]+',str(dirs),0)
            if (found and (lastdirectory <= int(dirs))):
                directorylist.append(int(dirs))
        directorylist = sorted(directorylist)
        splitlist = list(itertools.izip_longest(*(iter(directorylist),) * folders))
        logger_error.debug(splitlist)
        for split in splitlist:
            manager.reset_projkeys()
            foldlist = list()
            foldercompletelist = {}
            folderstartedlist = {}
            logger_error.debug("Getting the Folders List")
            for dirs in split:
                if(dirs == None):
                    continue
                for curr_dir, sub_dir, filenames in os.walk(directory+'/'+str(dirs)):
                            strg = curr_dir
                            foldlist.append(strg)
            logger_error.debug("Folders List:")
            n = len(foldlist)
            logger_error.debug("Starting Processes:")
            songs_pool = Pool()
            songs_pool =Pool(processes=m1)
            if(crawlyoutube == 0):
                songs_pool.imap(crawlArtist,foldlist)
            else:
                songs_pool.imap(runYoutubeApi,foldlist)
                
            songs_pool.close()
            songs_pool.join()
            print datetime.now()-t1
            logger_error.debug("completed for split : "+','.join(map(str,split)))
            fwrite.write(str(split[0]))
            fwrite.write("\n")
            if(split[-1]!= None):
                fwrite.write(str(split[-1]))
                logger_decisions.error(str(split[-1]))
                fwrite.write("\n")
        fwrite.close()
    except Exception as e:
        logger_error.exception(e)
    t2=datetime.now()
    print "time=" +str(t2-t1)