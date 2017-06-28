import sys
import loggingmodule
import subprocess
import os
import codecs
from solr import SolrConnection
from solr.core import SolrException
import operator
solrConnection = SolrConnection('http://aurora.cs.rutgers.edu:8181/solr/discogs_data_test')




def downloadYoutube(youtubeIds,foldername):
    try:
        #logger_download = loggingmodule.initialize_logger('downloadyoutube.log')
        #youtube-dl --extract-audio --audio-format wav -o "blues.%(autonumber)s.%(ext)s"  'https://www.youtube.com/watch?v=a3HemKGDavw' 'https://www.youtube.com/watch?v=EQPqyk9rPW4'
        common_url = 'https://www.youtube.com/watch?v='
        outputPattern = foldername+'/'+foldername+'.%(autonumber)s.%(ext)s'
        args = ['youtube-dl', '--extract-audio', '--audio-format', 'wav','-o',outputPattern,'-i']
        for ids in youtubeIds:
            args.append(common_url+ids)
        subprocess.call(args)
        
    except Exception as ex:
        logger_download.exception(ex)

def getYoutubeIds(genre):
    global solrConnection
    genreQuery = 'genreMatch:"'+str(genre)+ '"'
    intersect = 0
    retvalues = []
    extravalues = []
    try:
        response = solrConnection.query(q="*:*",fq=[genreQuery],version=2.2,wt = 'json',facet='true', facet_field='artistName',fl=['facet_fields'])
        intersect = int(response.results.numFound)
        if(intersect > 0):
            artist_dict = response.facet_counts['facet_fields']['artistName']
            print len(artist_dict)
            sorted_artist_dict = sorted(artist_dict.items(), key=operator.itemgetter(1),reverse = True)
            print sorted_artist_dict[0:10]
            for result in sorted_artist_dict[0:10]:
                #curr_artist = result['artistName'][0]
                
                '''http://aurora.cs.rutgers.edu:8181/solr/discogs_data_test/select?q=*%3A*&fq=genreMatch%3Ablueschicagoblues&fq=artistName%3A%22Muddy+Waters%22&fl=youtubeId&wt=json
                '''
                try:
                    curr_artist = result[0]
                    curr_query = 'artistName:"'+str(curr_artist) + '"'
                    response_ids = solrConnection.query(q="*:*",fq=[genreQuery,curr_query],version=2.2,wt = 'json',fl=['youtubeId','youtubeName'],rows=12)
                    intersect1 = int(response_ids.results.numFound)
                    if(intersect1 >0):
                        for res in response_ids.results:
                            print str(res['youtubeId']) + '-----' + str(res['youtubeName'])
                            retvalues.append(str(res['youtubeId']))

                except Exception as ex:
                    logger_download.exception(ex)
                
    
    except Exception as ex:
        logger_download.exception(ex)
    return retvalues


def getYoutubeIdsfromArtists(artistList):
    global solrConnection
    #genreQuery = 'genreMatch:"'+str(genre)+ '"'
    intersect = 0
    retvalues = []
    extravalues = []
    try:
        #response = solrConnection.query(q="*:*",fq=[genreQuery],version=2.2,wt = 'json',facet='true', facet_field='artistName',fl=['facet_fields'])
        #intersect = int(response.results.numFound)
        #if(intersect > 0):
            #artist_dict = response.facet_counts['facet_fields']['artistName']
            #print len(artist_dict)
            #sorted_artist_dict = sorted(artist_dict.items(), key=operator.itemgetter(1),reverse = True)
            #print sorted_artist_dict[0:10]
            for result in artistList:
                #curr_artist = result['artistName'][0]
                print result
                '''http://aurora.cs.rutgers.edu:8181/solr/discogs_data_test/select?q=*%3A*&fq=genreMatch%3Ablueschicagoblues&fq=artistName%3A%22Muddy+Waters%22&fl=youtubeId&wt=json
                '''
                try:
                    curr_artist = result
                    curr_query = 'artistName:"'+str(curr_artist) + '"'
                    response_ids = solrConnection.query(q="*:*",fq=[curr_query],version=2.2,wt = 'json',fl=['youtubeId','youtubeName'],rows=12)
                    intersect1 = int(response_ids.results.numFound)
                    if(intersect1 >0):
                        for res in response_ids.results:
                            print res
                            print str(res['youtubeId']) + '-----' + str(res['youtubeName'])+ '----'+str(res['youtubeName'])
                            retvalues.append(str(res['youtubeId']))

                except Exception as ex:
                    logger_download.exception(ex)
                
    
    except Exception as ex:
        logger_download.exception(ex)
    return retvalues




if __name__ == '__main__':
    reload(sys)
    logger_download = loggingmodule.initialize_logger('downloadyoutube.log')
    sys.setdefaultencoding('utf8')
    foldername = 'blueschicagoblues'
    matrixtype = int(raw_input('Enter 1 for artist list or 0 for genrename\n'))
    if(matrixtype==0):
        genrename = raw_input('Enter genreName\n')
        foldername = genrename        
        idslist = getYoutubeIds('blueschicagoblues')
    else:
        filename = raw_input('Enter filename\n')
        foldername = filename[:filename.rfind('.')]
        if os.path.exists(filename):
            fileopen = codecs.open(filename,"r","utf-8")
            lines = fileopen.readlines()
            lines = filter(lambda x: x.replace('\n','') != '',lines)
            lines = map(lambda x:x.replace('\n',''),lines)
        idslist = getYoutubeIdsfromArtists(lines)
    downloadYoutube(idslist,foldername)
    '''filename = raw_input("Enter filename: ")
    foldername = filename[:filename.rfind('.')]
    if not os.path.exists(foldername):
        os.makedirs(foldername)
    if os.path.exists(filename):
        fileopen = codecs.open(filename,"r","utf-8")
        lines = fileopen.readlines()
        lines = filter(lambda x: x.replace('\n','') != '',lines)
        lines = map(lambda x:x.replace('\n',''),lines)
        downloadYoutube(lines,foldername)
    else:
        print 'File doesnt exist' '''
    #getYoutubeIds('blueschicagoblues')
