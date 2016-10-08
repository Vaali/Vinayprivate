import os
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import codecs
import scipy.sparse as sp
import numpy as np
import loggingmodule
from itertools import repeat
from datetime import datetime, date, timedelta
from multiprocessing import Pool
import similar_artists_api as sa
from sys import getsizeof





#London:;0JFdt_8QvYA:;1221768:;Lord Sutch And Heavy Friends:;10087:;1970-05-25:;Rock:;0:;0
#songname:;youtubeId:;artistid:;artistname:;popularity:;year:;genre:;songid:;genreid

def getmatrixdata_artists(filename,row_max_artists,col_max_artists):
	fileopen = codecs.open(filename,"r","utf-8")
	lines = fileopen.readlines()
        lines = filter(lambda x: x.replace('\n','') != '',lines)
	lines = map(lambda x:x.replace('\n',''),lines)
	print len(lines)
        remap_artist_map = {}
        curr_artist_count = 0
        remapped_lines = []
        for line in lines:
            if(line != ''):
                words = line.split(':;')
                curr_col = int(words[2])
                if(curr_col not in remap_artist_map):
                    remap_artist_map[curr_col] = curr_artist_count
                    curr_artist_count = curr_artist_count + 1
                line = line +':;'+str(remap_artist_map[curr_col])
                remapped_lines.append(line)
        #print remapped_lines[0]
	for line in remapped_lines:
            if(line != ''):
                words = line.split(':;')
                curr_row = int(words[7])
                curr_col = int(words[9])
                if(curr_row not in songs_map):
                    #youtubeId,artistId,artistName,popularity,year,genre
                    songs_map[curr_row] = [words[0],words[1],words[2],words[3],words[4],words[5].split('-')[0],words[6]]
                    popularity_list_songs[curr_row] = int(words[4])

                    #rows_popularity_list.append(0)
                    #cols_popularity_list.append(curr_row)
                    #data_popularity_list.append(int(words[5]))
                if(curr_col not in artists_map):
                    popularity_list[curr_col] = int(words[4])
                    artists_map[curr_col] = [words[1],words[2],words[3],words[4],words[5].split('-')[0],words[6]]
                else:
                    popularity_list[curr_col] += int(words[4])

                if(row_max_artists < curr_row):
                    row_max_artists = curr_row
                column_list_artists.append(curr_col) #remapped column
                rows_list_artists.append(curr_row)
                data_list_artists.append(1)
        print len(artists_map)
        return row_max_artists,curr_artist_count-1


def getmatrixdata_genres(filename,row_max_genres,col_max_genres):
	fileopen = codecs.open(filename,"r","utf-8")
	lines = fileopen.readlines()
        lines = filter(lambda x: x.replace('\n','') != '',lines)
	lines = map(lambda x:x.replace('\n',''),lines)
	for line in lines:
            if(line != ''):
                words = line.split(':;')
                curr_row = int(words[7])
                curr_col = int(words[8])
                if(curr_row not in songs_map):
                    songs_map[curr_row] = [words[0],words[1],words[2],words[3],words[4],words[5].split('-')[0],words[6]]
                    #rows_popularity_list.append(0)
                    #cols_popularity_list.append(curr_row)
                    #data_popularity_list.append(1)
                if(curr_col not in genres_map):
                    genres_map[curr_col] = words[6]
                if(row_max_genres < curr_row):
                    row_max_genres = curr_row
                if(col_max_genres < curr_col):
                    col_max_genres = curr_col
                column_list_genres.append(curr_col)
		data_list_genres.append(1)
                rows_list_genres.append(curr_row)

        return row_max_genres,col_max_genres

#delete later

def sort_rows(m):
    #print m
    m.data *= m.data>=0.89
    m.eliminate_zeros()
    #print 'xxxxxxxxxxx'
    #print m
    rows=  m.nonzero()[0]
    cols = m.nonzero()[1]
    #for y in cols:
    #    print m[0,y]
    t3=datetime.now()
    temp_ones = m.copy()
    temp_ones.data /= temp_ones.data
    sample = popularitymatrix.multiply(temp_ones)
    popmat = popularitymatrix.tocsr()
    tuples =  zip(sample.indices,sample.data)
    t4=datetime.now()
    #print 'zip time' + str(t4-t3)
    #tuples_filtered = filter(lambda x: x[1]>0.89, finaltuples)
    t5=datetime.now()
    #print len(tuples_filtered)
    sorted_x = sorted(tuples, key=lambda score: score[1], reverse=True)
    t6=datetime.now()
    #print 'sort time' + str(t6-t5)
    #print sorted_x
    return sorted_x

def sort_rows_songs(m):
    m.data *= m.data>=1
    m.eliminate_zeros()
    #print 'xxxxxxxxxxx'
    #print m
    rows=  m.nonzero()[0]
    cols = m.nonzero()[1]
    #for y in cols:
    #    print m[0,y]
    t3=datetime.now()
    temp_ones = m.copy()
    temp_ones.data /= temp_ones.data
    sample = popularitymatrixsongs.multiply(temp_ones)
    popmat = popularitymatrixsongs.tocsr()
    tuples =  zip(sample.indices,sample.data)
    t4=datetime.now()
    #print 'zip time' + str(t4-t3)
    #tuples_filtered = filter(lambda x: x[1]>0.89, finaltuples)
    t5=datetime.now()
    #print len(tuples_filtered)
    sorted_x = sorted(tuples, key=lambda score: score[1], reverse=True)
    t6=datetime.now()
    #print 'sort time' + str(t6-t5)
    #print sorted_x
    return sorted_x


def similarartist((split,block_indices,index)):
    try:
        #global tempA2
        
        cosinesimilarityartist = split[0]*tempA2
        row_indices = np.split(cosinesimilarityartist.indices, cosinesimilarityartist.indptr[1:-1])
        logger_matrix.exception('writing the artists files')
        indices = zip(range(block_indices[index],block_indices[index+1]),range(block_indices[index+1]-block_indices[index]))
        #print cosinesimilarityartist
        #print indices
        #youtubeId,artistId,artistName,popularity,year,genre
        for (artist_index,sim_mat_index) in indices:
            t1 = datetime.now()
            curr_artistName = artists_map[artist_index][2]
            curr_artist_id = int(artists_map[artist_index][1])
            curr_artist_popularity = int(artists_map[artist_index][3])
            curr_artist_year = int(artists_map[artist_index][4])
            curr_artist = sa.artist()
            curr_artist.set_artistName(curr_artistName)
            curr_artist.set_artistId(curr_artist_id)
            curr_artist.set_artistPopularityAll(curr_artist_popularity)
            curr_artist.set_earliestDate(curr_artist_year)
            written = 0
            #currentrow = cosinesimilarityartist[sim_mat_index,:].toarray()
            currentrow = cosinesimilarityartist.getrow(sim_mat_index)
            #print currentrow
            t3=datetime.now()
            sorted_s = sort_rows(cosinesimilarityartist.getrow(sim_mat_index))
            t4=datetime.now()
            #print (t4-t3)
            #change here to increase the number of similar artists
            sorted_s = sorted_s[0:100]
	    #for j in row_indices[sim_mat_index]:
            for pair in sorted_s:
                j = pair[0]
                #if(currentrow[0][j] > 0.89):
                #if(pair[1] > 0.89):
                written = 1
                curr_similar_artist = artists_map[j][2]
                curr_similar_artist_id = int(artists_map[j][1])
                curr_similar_artist_popularity = int(artists_map[j][3])
                curr_similar_artist_year = int(artists_map[j][4])
                similar_artists = sa.similarArtists()
                similar_artists.set_artistName(curr_similar_artist)
                similar_artists.set_artistId(curr_similar_artist_id)
                similar_artists.set_artistPopularityAll(curr_similar_artist_popularity)
                similar_artists.set_earliestDate(curr_similar_artist_year)
                similar_artists.set_cosineDistance(currentrow[0,j])
                #similar_artists.set_euclideanDistance(euclideandistanceartist[i][j])
                #similar_artists.set_pearsonDistance(pearsondistanceartist[i,:][j])
                curr_artist.add_similarArtists(similar_artists)
            if(written == 1):
                fname = 'simartistdir/' + str(curr_artist_id)+'.xml'
                fx = codecs.open(fname,"w","utf-8")
                fx.write('<?xml version="1.0" ?>\n')
                curr_artist.export(fx,0)
                fx.close()
            t2=datetime.now()
            print (t2-t1)
    except Exception as e:
	logger_matrix.exception(e)

def similarsongs((split,block_indices,index)):
    #global tempA2
    try:
        #multiplying in blocks of matrix
        cosinesimilaritysong = split[0]*tempA2
        row_indices = np.split(cosinesimilaritysong.indices, cosinesimilaritysong.indptr[1:-1])
        logger_matrix.exception('writing the artists files')
        indices = zip(range(block_indices[index],block_indices[index+1]),range(block_indices[index+1]-block_indices[index]))
        #print cosinesimilarityartist
        #print indices
        #songname,youtubeId,artistId,artistName,popularity,year,genre
        curr_xmls = {}
        for (song_index,sim_mat_index) in indices:
            t1 = datetime.now()
            curr_songName = songs_map[song_index][0]
            curr_song_id = songs_map[song_index][1]
            curr_song_popularity = int(songs_map[song_index][4])
            curr_song_year = int(songs_map[song_index][5])
            curr_artist_id = int(songs_map[song_index][2])
            curr_artist_name = songs_map[song_index][3]
            curr_song = sa.song()
            curr_song.set_songName(curr_songName)
            curr_song.set_songId(curr_song_id)
            curr_song.set_songPopularityAll(curr_song_popularity)
            curr_song.set_earliestDate(curr_song_year)
            curr_song.set_artistName(curr_artist_name)
            curr_song.set_artistId(curr_artist_id)

            written = 0
            #currentrow = cosinesimilarityartist[sim_mat_index,:].toarray()
            currentrow = cosinesimilaritysong.getrow(sim_mat_index)
            #print currentrow
            t3=datetime.now()
            sorted_s = sort_rows_songs(cosinesimilaritysong.getrow(sim_mat_index))
            t4=datetime.now()
            print 'sorttime ' + str(t4-t3)
            #change here to increase the number of similar artists
            sorted_s = sorted_s[0:100]
	    #for j in row_indices[sim_mat_index]:
            for pair in sorted_s:
                j = pair[0]
                #if(currentrow[0][j] > 0.89):
                #if(pair[1] > 0.89):
                written = 1
                curr_similar_song = songs_map[j][0]
                curr_similar_song_id = songs_map[j][1] + ' ' + str(songs_map[j][5])
                curr_similar_song_popularity = int(songs_map[j][4])
                curr_similar_song_year = int(songs_map[j][5])
                similar_songs = sa.similarSongs()
                similar_songs.set_songName(curr_similar_song)
                similar_songs.set_artistName(songs_map[j][3])
                similar_songs.set_artistId(int(songs_map[j][2]))
                similar_songs.set_songId(curr_similar_song_id)
                similar_songs.set_songPopularityAll(curr_similar_song_popularity)
                similar_songs.set_earliestDate(curr_similar_song_year)
                similar_songs.set_cosineDistance(currentrow[0,j])
                #similar_artists.set_euclideanDistance(euclideandistanceartist[i][j])
                #similar_artists.set_pearsonDistance(pearsondistanceartist[i,:][j])
                curr_song.add_similarSongs(similar_songs)
            if(written == 1):
                #fname = 'simsongsdir/0000' + str(curr_song_id)+'.xml'
                #fx = codecs.open(fname,"w","utf-8")
                #fx.write('<?xml version="1.0" ?>\n')
                #curr_song.export(fx,0)
                #fx.close()
                curr_xmls[curr_song_id] = curr_song
        for song_xml in curr_xmls:
            fname = 'simsongsdir/0000' + str(song_xml)+'.xml'
            fx = codecs.open(fname,"w","utf-8")
            fx.write('<?xml version="1.0" ?>\n')
            curr_xmls[song_xml].export(fx,0)
            fx.close()
        t2=datetime.now()
        print 'writing time '+str(t2-t4)

    except Exception as e:
	logger_matrix.exception(e)

def similargenre((i,cosinesimilaritygenre)):
    try:
	curr_genreName = genres_map[i]
	curr_genre_id = i
	#curr_artist_popularity = int(artists_map[i][1])
	#curr_artist_year = int(artists_map[i][2])
	fname = 'simgenredir/' + str(curr_genre_id)+'.xml'
	fx = codecs.open(fname,"w","utf-8")
	fx.write('<?xml version="1.0" ?>\n')
	curr_genre = sa.artist() 
	curr_genre.set_artistName(curr_genreName)
	curr_genre.set_artistId(curr_genre_id)
        #curr_genre.set_artistPopularityAll(curr_artist_popularity)
	#curr_genre.set_earliestDate(curr_artist_year)
	#change this to correct sparse matrix manipulations
        curr_row = cosinesimilaritygenre[i,:]
        #print curr_row
        curr_row.data *= curr_row.data>=0.85
        curr_row.eliminate_zeros()
        tuples =  zip(curr_row.indices,curr_row.data)
        sorted_g = sorted(tuples, key=lambda score: score[1], reverse=True)
        sorted_g = sorted_g[0:100]      
	for pair in sorted_g:
            j = pair[0]
            curr_similar_genre = genres_map[j]
	    curr_similar_artist_id = j
            similar_genre = sa.similarArtists()
	    similar_genre.set_artistName(curr_similar_genre)
	    similar_genre.set_artistId(curr_similar_artist_id)
	    similar_genre.set_cosineDistance(pair[1])
	    curr_genre.add_similarArtists(similar_genre)
	curr_genre.export(fx,0)
	fx.close()
    except Exception as e:
	logger_matrix.exception(e)

def cosine_similarity_genres(genresongsmatrix):
    try:
        G1 = genresongsmatrix.transpose().tocsr()
        row_sums_genres = ((G1.multiply(G1)).sum(axis=1))
        
        rows_sums_sqrt_genres = np.array(np.sqrt(row_sums_genres))[:,0]
        row_indices, col_indices = G1.nonzero()
        #print col_indices
        #print tempG1.data[2] 
        G1.data = G1.data/rows_sums_sqrt_genres[row_indices]
        #print tempG1.todense()
        G2 = G1.transpose()
        cosinesimilaritygenre = G1*G2
        logger_matrix.exception('writing the genre files')
        g =Pool(processes=int(100))
        g.map(similargenre,zip(genres_map.keys(),repeat(cosinesimilaritygenre)))
        g.close()
        g.join()

    except Exception as e:
	logger_matrix.exception(e)



def split_sparse(mat, row_divs = [], col_divs = []):
    '''
    mat is a sparse matrix
    row_divs is a list of divisions between rows.  N row_divs will produce N+1 rows of sparse matrices
    col_divs is a list of divisions between cols.  N col_divs will produce N+1 cols of sparse matrices

    return a 2-D array of sparse matrices
    '''
    row_divs = [None]+row_divs+[None]
    col_divs = [None]+col_divs+[None]
    print row_divs
    print col_divs
    mat_of_mats = np.empty((len(row_divs)-1, len(col_divs)-1), dtype = type(mat))
    for i, (rs, re) in enumerate(zip(row_divs[:-1], row_divs[1:])):
        for j, (cs, ce) in enumerate(zip(col_divs[:-1], col_divs[1:])):
            mat_of_mats[i, j] = mat[rs:re, cs:ce]

    return mat_of_mats

def cosine_similarity(tempA1,row_max,isSongs):
    try:
        global tempA2
        print tempA1.shape
        print 'getsizeof'
        print tempA1.data.nbytes
        #calculating the row sums 
        row_sums = ((tempA1.multiply(tempA1)).sum(axis=1))
        #calculating the sqrt of the sums
        rows_sums_sqrt = np.array(np.sqrt(row_sums))[:,0]
        #divide and get the norms
        row_indices, col_indices = tempA1.nonzero()
        tempA1.data = tempA1.data/rows_sums_sqrt[row_indices]
        
        tempA2 = tempA1.transpose()
        #change this to correct sparse matrix manipulations

        #break the matrix into peices
        #if(tempA1.shape[0]<100):
        #    block_indices = range(1,tempA1.shape[0])
        #else:
        block_indices = range(100,tempA1.shape[0],100)
        #function returns the blocks of the main matrix        
        split_mat = split_sparse(tempA1,block_indices,[])
        
        index = 0
        block_indices = [0]+ block_indices + [row_max]
        #foreach block returned calculate the cosine similarity 
        print 'there'
        print row_max
        #similarartist((split_mat[0],block_indices,0))
        p =Pool(processes=int(25))
        if(isSongs ==0):
	    p.map(similarartist,zip(split_mat,repeat(block_indices),range(0,len(block_indices))))
        else:
            p.map(similarsongs,zip(split_mat,repeat(block_indices),range(0,len(block_indices))))

        p.close()
	p.join()
        print 'cosine distance'
    except Exception as e:
	logger_matrix.exception(e)

#end here
#since the huge matrix is a readonly shared variable no need of passing it to the new process as the child pocess keeps a memory map of the main process
tempA2 = []
if __name__ == '__main__':
    logger_matrix = loggingmodule.initialize_logger('computesimilarities.log')
    t1=datetime.now()
    #global tempA2
    try:
        if(not os.path.exists('simartistdir')):
	    os.mkdir('simartistdir')
        if(not os.path.exists('simgenredir')):
	    os.mkdir('simgenredir')
        if(not os.path.exists('simsongsdir')):
	    os.mkdir('simsongsdir')

        isSongs = int(raw_input("Enter artists/songs: "))
        column_list_artists = []
        rows_list_artists = []
        data_list_artists = []
        
        column_list_genres = []
        rows_list_genres = []
        data_list_genres = []
        
        
        rows_popularity_list = []
        cols_popularity_list = []
        data_popularity_list = []

        rows_popularity_list_songs = []
        cols_popularity_list_songs = []
        data_popularity_list_songs = []
        popularity_list = {}
        popularity_list_songs = {}
        col_max_artists = 0
        row_max_artists = 0
        col_max_genres = 0
        row_max_genres =0
        
        earlier_year = 2050
        songs_map = {}
        genres_map = {}
        artists_map = {}
        row_max_artists,col_max_artists = getmatrixdata_artists('remapped_songs_file.txt',row_max_artists,col_max_artists)
        row_max_genres,col_max_genres = getmatrixdata_genres('remapped_songs_file.txt',row_max_genres,col_max_genres)
        #popularity
        for curr_row in popularity_list:
            rows_popularity_list.append(0)
            cols_popularity_list.append(curr_row)
            data_popularity_list.append(popularity_list[curr_row])
        popularitymatrix = sp.coo_matrix((data_popularity_list,(rows_popularity_list,cols_popularity_list)),shape=(1,col_max_artists+1))
        for curr_row in popularity_list_songs:
            rows_popularity_list_songs.append(0)
            cols_popularity_list_songs.append(curr_row)
            data_popularity_list_songs.append(popularity_list_songs[curr_row])

        popularitymatrixsongs = sp.coo_matrix((data_popularity_list_songs,(rows_popularity_list_songs,cols_popularity_list_songs)),shape=(1,row_max_artists+1))

        row_art  = np.array(rows_list_artists)
        col_art  = np.array(column_list_artists)
        data_art = np.array(data_list_artists)
        artistsongsmatrix = sp.coo_matrix((data_art, (row_art, col_art)), shape=(row_max_artists+1, col_max_artists+1))
        
        row_genre  = np.array(rows_list_genres)
        col_genre  = np.array(column_list_genres)
        data_genre = np.array(data_list_genres)
        genresongsmatrix = sp.coo_matrix((data_genre, (row_genre, col_genre)), shape=(row_max_genres+1, col_max_genres+1))
        
        print artistsongsmatrix.shape
        print genresongsmatrix.shape
 
        G = genresongsmatrix.tocsr()
        A = artistsongsmatrix.transpose().tocsr()
        A.data = A.data/A.data
        #print A.todense()
        AGP = A*G
        #print AGP.todense()
        AG = AGP.tocsr()
        #Cosine similarity
        #print tempA1.shape
        #calculating the row sums

        del artistsongsmatrix
        row_sums = ((AG.multiply(AG)).sum(axis=1))
        #calculating the sqrt of the sums
        rows_sums_sqrt = np.array(np.sqrt(row_sums))[:,0]

        #divide and get the norms
        row_indices, col_indices = AG.nonzero()

        AG.data = AG.data/rows_sums_sqrt[row_indices]
        

        #sim_mat = AG * AGT
        #print sim_mat[1,:]
        if(isSongs == 0):
            cosine_similarity(AG,AG.shape[0],isSongs)
        else:
            cosine_similarity(G,G.shape[0],isSongs)

        
        cosine_similarity_genres(genresongsmatrix)
        t2=datetime.now()
        print (t2-t1)

    except Exception as e:
	logger_matrix.exception(e)



