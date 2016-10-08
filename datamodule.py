import sys
import codecs

#utility functions

def get_year_from_title(vid_title):
    return_year = 0
    year_list = re.findall(r'\d\d\d\d+',vid_title)
    if(len(year_list) != 0):
        return_year = int(year_list[0])
        if(vid_title == year_list[0]):
            return_year = 0
    return return_year

def get_year_from_releasedate(releasedate):
    if(releasedate == None or releasedate != ''):
        return 1001
    curr_year = releasedate.split('-')[0] 
    if(curr_year != ''):
        return int(curr_year)
    else:
        return 1001




#classes



class Artist:
    def __init__(self,artist_name = "",artist_id= 0,position = 1,is_featured_artist = False):
        self.name = artist_name
        self.artist_id = artist_id
        self.unique_id = artist_name + '_' + str(artist_id)
        self.is_featured_artist = is_featured_artist
        self.postion = position
        self.country = "US"
        self.year = 1001
        self.popularity = 0


class Genre:

    def __init__(self,genre_name):
        self.genre_name = genre_name

class Album:
    def __init__(self,album_name = "",album_year = 1001,is_release = False):
        self.album_name = album_name
        self.album_year = album_year
        self.is_release = is_release
        self.genres = []
        self.styles = []
        self.Artists = []


    def set_genres(self,genres_list):
        for genre in genres_list:
            curr_genre = Genre(genre)
            self.genres.append(curr_genre)

    def set_styles(self,styles_list):
        for style in styles_list:
            curr_style = Genre(style)
            self.genres.append(curr_style)

    def is_release_collection(self):
        ret = False
        for format1 in self.formats:
            if(format == None):
                continue
            descriptions = format1['descriptions']
            for desc in descriptions:
                desc = desc.lower()
                if(desc == "collections" or desc == "mixed" or desc == "compilation"):
                    ret = True
        self.isrelease = ret

    def get_released_date(self,release_date):
        ''' 20041109 '''
        if('-' in release_date):
            return release_date
        if(len(release_date) == 4):
            return release_date
        retstring = ""
        retstring = release_date[0:4]
        retstring = retstring + '-'
        retstring = retstring + release_date[4:6]
        retstring = retstring + '-'
        retstring = retstring + release_date[6:8]
        return retstring
    
    def parse_album(self,file_name):
        curr_album = {}
        with codecs.open(file_name,"r","utf-8") as input1:
            curr_album = json.load(input1)
        self.is_release_collection()
        release_date = ''
        if(curr_album['released_date'] != None):
            release_date = self.get_released_date(curr_album['released_date'])
        self.release_date = release_date
        self.set_styles(curr_album['styles'])
        self.set_genres(curr_album['genres'])
        self.country = curr_album['country']
        self.tracks = curr_album['tracks']
        self.year = get_year_from_release_date(release_date)
        self.release_id = curr_album['release_id']
        self.album_name = curr_album['title']
        for artist in curr_album['releaseartists']:
            if(artist == None):
                continue
            artist['artist_name'] = re.sub(r'\(.*?\)', '', artist['artist_name']).strip().lower()
            if(', the' in artist['artist_name'].lower()):
                artist['artist_name'] = artist['artist_name'].lower().replace(', the','')
                artist['artist_name'] = 'the '+ artist['artist_name']
            if(artist['position'] == 1):
                        song['artist_name'] = re.sub(r'\(.*?\)', '', artist['artist_name'].lower()).strip()
                        #print song['artistName']
                        #print artist['artist_id']
                        song['artist_id'] = artist['artist_id']
                        #add anvs for the main artist alone
                        if('anv' in artist and artist['anv'] != None):
                            song['anv'] = artist['anv']
                        if(artist['join_relation'] != None):
                            song['connectors'].append(artist['join_relation'])
                        elif(artist['artist_name'].lower() not in song['featArtists'] and ('artistName' not in song or (artist['artist_name'].lower() != song['artistName'].lower()))):
                            song['featArtists'].append(artist['artist_name'].lower())
                            if(artist['join_relation'] != None):
                                song['connectors'].append(artist['join_relation'])







        

