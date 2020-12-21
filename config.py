#number of proceses
NumberOfProcesses = 20
NumberofThreads = 20

#incremental/full
IsIncremental = 0

IsCrawlingYoutube = 1

#crawling freshness
SkipRecentlyCrawledDirectories = 0
CrawlDaysWindow = 19


#step 1: get uniques songs list from discogs data
#getsongslist.py
#step2: crawl youtube to get videos
#getvideosfinal.py
#step 3: generate xmls from the dump file.
#generatexmls_discogs.py
DiscogsDataDirectory = 'smalldata'
NumberofFolders = 2
IsSoundCloud = 0
# 1 for using youtubeapi 0 for using youtube dl
IsYoutudeApi = 0
CacheDir = '/Volumes/Secondone/sourcefiles/solr_newData11_old/newdir/cache'

TopSongs = 10 # used to calculat averages of counts etc


#updatexml_new.py
# 1 for updating viewcounts , 0 for updating genre tags
IsUpdateViewCounts = 1
#directory where xmls are present
DataDirectory = 'solr_newData1222/deletedvideos'


#recrawling
#RecrawlDirectory = '/Volumes/Secondone/sourcefiles/solr_newData11_old/deletedvideos'
#RecrawlOutputDirectory = '/Volumes/Secondone/sourcefiles/solr_newData11_old/newdir/tt'
RecrawlDirectory = 'solr_newData1122'
RecrawlOutputDirectory = 'solr_newData1122/xxxx'

#solr url connections
SolrDataUrl = 'http://aurora.cs.rutgers.edu:8181/solr/discogs_data_test'
SolrDiscogsArtistsUrl = 'http://aurora.cs.rutgers.edu:8181/solr/discogs_artists'
SolrGenresUrl = 'http://aurora.cs.rutgers.edu:8181/solr/similar_genres'
SolrGenreTagsUrl = 'http://aurora.cs.rutgers.edu:8181/solr/genretags'
SolrSimilarArtistsUrl = 'http://aurora.cs.rutgers.edu:8181/solr/similar_artists1'








