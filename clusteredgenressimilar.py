''' 
Create the clustered genres similar groups and write them to a file
'''


import os
import sys
import re
import codecs
import json
from datetime import datetime, date, timedelta
import scipy.sparse as sp
import numpy as np
from multiprocessing import Pool
import similar_artists_api as sa





reload(sys)
sys.setdefaultencoding('utf8')
songs_map = {}
'''
Reads the genresmatrix json and creates the genres_dict
'''
def get_genres_matrix():
    genres_list=[]
    with codecs.open('genresmatrix.json', 'r') as f:
        genres_list = json.load(f)
    genres_dict = dict(map(reversed, genres_list.items()))
    #genres_dict = {v:k for k,v in genres_list.iteritems()}
    return genres_dict

'''
split the sparse matrix into parts
'''

def split_sparse(mat, row_divs = [], col_divs = []):
    '''
    mat is a sparse matrix
    row_divs is a list of divisions between rows.  N row_divs will produce N+1 rows of sparse matrices
    col_divs is a list of divisions between cols.  N col_divs will produce N+1 cols of sparse matrices

    return a 2-D array of sparse matrices
    '''
    row_divs = [None]+row_divs+[None]
    col_divs = [None]+col_divs+[None]
    #print row_divs
    #print col_divs
    mat_of_mats = np.empty((len(row_divs)-1, len(col_divs)-1), dtype = type(mat))
    for i, (rs, re) in enumerate(zip(row_divs[:-1], row_divs[1:])):
        for j, (cs, ce) in enumerate(zip(col_divs[:-1], col_divs[1:])):
            mat_of_mats[i, j] = mat[rs:re, cs:ce]

    return mat_of_mats

'''
Read the songs to genres files and create the sparse matrix.
'''
def getmatrixdata_genres(filename,row_max_genres,col_max_genres):
        t1=datetime.now()
	fileopen = codecs.open(filename,"r","utf-8")
	lines = fileopen.readlines()
        lines = filter(lambda x: x.replace('\n','') != '',lines)
	lines = map(lambda x:x.replace('\n',''),lines)
	for line in lines:
            if(line != ''):
                #print line
                words = line.split(':;')
                curr_row = int(words[7])
                curr_col = int(words[8])
                if(curr_row not in songs_map):
                    songs_map[curr_row] = [words[0],words[1],words[2],words[3],words[4],words[5].split('-')[0],words[6]]
                if(row_max_genres < curr_row):
                    row_max_genres = curr_row
                column_list_genres.append(curr_col)
		data_list_genres.append(1)
                rows_list_genres.append(curr_row)
        t3=datetime.now()
        print 'reading time'
        print str(t3-t1)
        return row_max_genres,col_max_genres

'''
Reads the songs to genres matrix and creates the combinedgenresmatrix which is read by the function generateCombinedMatrix.
'''

def getCombinedgenres():
    try:
        genres_dict = get_genres_matrix()
        songs_list = {}
        unique_genres = {}
        fileopen = codecs.open('remapped_songs_file_big.txt',"r","utf-8")
        lines = fileopen.readlines()
        lines = filter(lambda x: x.replace('\n','') != '',lines)
        lines = map(lambda x:x.replace('\n',''),lines)
        remapped_lines = []
        print 'remapping line'
        t1=datetime.now()
        for line in lines:
            words = line.split(':;')
            line = line.replace(':;'+words[8],':;'+str(genres_dict[words[6].lower()]))
            if(words[1] not in songs_list):
                songs_list[words[1]] = [words[6].lower()]
            else:
                songs_list[words[1]].append(words[6].lower())

            remapped_lines.append(line)
        
        fileopen.close()
        t2=datetime.now()
        print (t2-t1)
        filewrite = codecs.open('remapped_artist_sample.txt',"w","utf-8")	
        for line in remapped_lines:
	    filewrite.write(line)
	    filewrite.write('\n')
        print 'Counting starts'

        combined_songs_list = {}
        for song in songs_list:
            current_genres = songs_list[song]
            current_genres = sorted(current_genres)
            combinedgenrestring = '@'.join(current_genres)
            combinedgenrestring = combinedgenrestring.lower()
            if(combinedgenrestring not in unique_genres):
                unique_genres[combinedgenrestring] = [song]
            else:
                unique_genres[combinedgenrestring].append(song)
        with codecs.open('combinedgenresmatrix.json', 'w') as f:
            json.dump(unique_genres,f)
        t3=datetime.now()
        print (t3-t2)
    except Exception as e:
        print e


'''
Creates the similar matrix for genres.
'''
def cosine_similarity_genres(genresongsmatrix):
    try:
        t3=datetime.now()
        G1 = genresongsmatrix.transpose().tocsr()
        row_sums_genres = ((G1.multiply(G1)).sum(axis=1))
        #print G1.todens()
        rows_sums_sqrt_genres = np.array(np.sqrt(row_sums_genres))[:,0]
        row_indices, col_indices = G1.nonzero()
        #print col_indices
        #print tempG1.data[2] 
        G1.data = G1.data/rows_sums_sqrt_genres[row_indices]
        #print tempG1.todense()
        G2 = G1.transpose()
        cosinesimilaritygenre = G1*G2
        print cosinesimilaritygenre.shape
        return cosinesimilaritygenre
    except Exception as e:
        print e
	#logger_matrix.exception(e)

'''
Change the values of missing genres value into the sparse matrix in the current block
'''
def charePartialMatrix(curr_block):
    try:
        global cosinesimilaritygenre
        curr_index = curr_block[3]
        d1 = curr_block[0][0]
        dd3 = curr_block[1][0]
        dd4 = curr_block[2][0]
        dd3_rows = dd3.shape[0]
        d1_coo_matrix = d1.tocoo()

        current_stratindex = curr_index*1000
        for i in range(0,dd3_rows):
            totalindices = dd3.getrow(i).indices
            changeindices = dd4.getrow(i).indices
            '''if(i == 0):
                print d1.getrow(i)
                print d1.getrow(1)
                print totalindices
                print changeindices'''
            d6 = cosinesimilaritygenre[dd4.getrow(i).indices]
            #d5 = cosinesimilaritygenre[d3.getrow(1).indices]
            if(len(totalindices) == 1):
                continue
            d7 = (d6[:,totalindices].sum(axis=1) - 1)/(len(totalindices) - 1)
            count = len(changeindices)
            for j in range(0,count):
                val = d7[j,0]
                col = changeindices[j]
                '''if(col == 31):
                    print 'change'
                    print val'''
                #d1[i,col] = val
                d1_coo_matrix.row = np.append(d1_coo_matrix.row,i)
                d1_coo_matrix.col = np.append(d1_coo_matrix.col,col)
                d1_coo_matrix.data = np.append(d1_coo_matrix.data,val)
        d1_csr_matrix = d1_coo_matrix.tocsr()
        #print d1_csr_matrix.getrow(0)
        #print d1_coo_matrix.data
        #print 'done'
        return [curr_index,d1_csr_matrix]
    except Exception as e:
        print e


'''
Add the missing values for the current row
'''
def changematrix(songscombinedgenresmatrix,curr_combined_row,cr):
    try:
        global cosinesimilaritygenre
        count = songscombinedgenresmatrix.shape[0]
        repeat_mat = [curr_combined_row[0],]*count
        repeat_mat_sp = sp.coo_matrix(repeat_mat)
        orig = songscombinedgenresmatrix.tocsr()
        d1_coo_matrix = songscombinedgenresmatrix.tocoo()
        d1 = songscombinedgenresmatrix.tocsr()
        d2 = repeat_mat_sp.tocsr()
        d3 = d1 + d2
        d4 = d2 - d1
        d3.data = d3.data/d3.data
        dt4 = d4.tocoo()
        #print dt4.col
        count = len(dt4.row)
        print count
        for i in range(0,count): 
            if(dt4.data[i] < 0):
                dt4.data[i] = 0
        dt4 = dt4.tocsr()
        dt4.eliminate_zeros()
        d3_rows = d3.shape[0]
        d3_cols = d3.shape[1]
        t3=datetime.now()
        split_indices = range(0,d3_rows,1000)
        blocks_matrix = split_sparse(d1,split_indices,[])
        blocks_total = split_sparse(d3,split_indices,[])
        blocks_diff = split_sparse(dt4,split_indices,[])
        #print blocks_total[1][0]
        #ret = charePartialMatrix((blocks_matrix[1],blocks_total[1],blocks_diff[1],0)) 
        g =Pool(processes=int(25))
        ret = g.map(charePartialMatrix,zip(blocks_matrix,blocks_total,blocks_diff,range(0,len(blocks_total))))
        g.close()
        g.join()
        sorted_list = sorted(ret, key = lambda x: int(x[0]))
        temp_test = sorted_list[0][1]

        #temp_test = sp.vstack([temp_test,temp_test])
        for i in sorted_list[1:]:
            temp_test = sp.vstack([temp_test,i[1]])
        #temp_test = ret[1]
        temp_test.eliminate_zeros()
        print temp_test.getrow(15845)
        print orig.getrow(0)
        #print [x[0] for x in ret]
        print 'there'
        t4=datetime.now()
        print str(t4-t3)
        print d1.shape
        d1.eliminate_zeros()
        print curr_combined_row.transpose().shape
        #d8 = d1*curr_combined_row.transpose()
        
        
        
        #cosine similarity
        row_sums = ((temp_test.multiply(temp_test)).sum(axis=1))
        #calculating the sqrt of the sums
        rows_sums_sqrt = np.array(np.sqrt(row_sums))[:,0]
        
        #divide and get the norms
        row_indices, col_indices = temp_test.nonzero()
        
        #rows_sums_sqrt
        temp_test.data = temp_test.data/rows_sums_sqrt[row_indices]
        
        tempA2 = temp_test.transpose().tocsc()
        print tempA2.shape
        curr_row = temp_test.getrow(cr)
        d8 = curr_row * tempA2
        d8.data *= d8.data>0.85
        d8.eliminate_zeros()

        #oldcosine similarity added fr testing
        '''
        row_sums = ((orig.multiply(orig)).sum(axis=1))
        #calculating the sqrt of the sums
        rows_sums_sqrt = np.array(np.sqrt(row_sums))[:,0]
        
        #divide and get the norms
        row_indices, col_indices = orig.nonzero()
        
        #rows_sums_sqrt
        orig.data = orig.data/rows_sums_sqrt[row_indices]
        
        tempA3 = orig.transpose().tocsc()
        curr_row = orig.getrow(cr)
        d9 = curr_row * tempA3
        d9.data *= d9.data>0.85
        d9.eliminate_zeros()

        print 'changematrix' '''
        return d8
    except Exception as e:
        print e


def generateCombinedMatrix():
    try:
        global cosinesimilaritygenre
        column_list_combinedgenres = []
        combinedgenresdictrev = {}
        rows_list_combinedgenres = []
        data_list_combinedgenres = []
        t1=datetime.now()
        with codecs.open('combinedgenresmatrix.json','r') as f:
            combinedgenresdict = json.load(f)
        print len(combinedgenresdict)
        genres_dict = get_genres_matrix();
        count = 0
        #adding the ids for the groups so that we can write it later
        for cgd in combinedgenresdict:
            curr_songs = combinedgenresdict[cgd]
            combinedgenresdict[cgd] = [curr_songs,count]
            combinedgenresdictrev[count] = cgd
            count = count + 1
        curr_list = [v[1] for v in combinedgenresdict.values()]
        row_max =0
        col_max = 0
        for cgd in combinedgenresdict:
            curr_genres = set(cgd.split('@'))
            curr_row = int(combinedgenresdict[cgd][1])
            if(row_max < curr_row):
                row_max = curr_row
            for curr_gen in curr_genres:
                curr_col = int(genres_dict[curr_gen])
                if(col_max < curr_col):
                    col_max = curr_col

                if(curr_row==0):
                    print curr_col
                column_list_combinedgenres.append(curr_col)
                rows_list_combinedgenres.append(curr_row)
                data_list_combinedgenres.append(1)
        row_comgenres  = np.array(rows_list_combinedgenres)
        col_comgenres  = np.array(column_list_combinedgenres)
        data_comgenres = np.array(data_list_combinedgenres)
        
        #row_max_comgenres = len(rows_list_combinedgenres)
        #col_max_comgenres = len(column_list_combinedgenres)
        #print col_max_comgenres
        #print row_max_comgenres
        songscombinedgenresmatrix = sp.coo_matrix((data_comgenres, (row_comgenres, col_comgenres)), shape=(row_max+1, col_max+1))
        t2=datetime.now()
        print 'matrix created'
        print str(t2-t1)
        cosinesimilaritygenre = cosine_similarity_genres(songscombinedgenresmatrix)
        t3=datetime.now()
        print 'genres matrix created'
        print str(t3-t2)
        cr = 3
        curr_comnined_row = songscombinedgenresmatrix.tocsr().getrow(cr).toarray()

        simi_genre = changematrix(songscombinedgenresmatrix,curr_comnined_row,cr)
        writeClusteredGenresxmls(simi_genre,cr,combinedgenresdictrev)
        '''for (ind,data) in zip(simi_genre.indices,simi_genre.data):
            print ind
            print data
            #print data1
            print combinedgenresdictrev[ind]
        print len(simi_genre.data)
        for (ind,data) in zip(simi_genre_old.indices,simi_genre_old.data):
            print ind
            print data
            print combinedgenresdictrev[ind]
        print len(simi_genre_old.data)'''
        #print (curr_list)
        
    except Exception as e:
        print e

'''
Writes the xmls for the current combined genre
'''

def writeClusteredGenresxmls(curr_row,cr,combinedgenresdictrev):
    try:
        curr_genreName = combinedgenresdictrev[cr]
	curr_genre_id = cr
	#curr_artist_popularity = int(artists_map[i][1])
	#curr_artist_year = int(artists_map[i][2])
	fname = 'simcombinedgenredir/' + str(curr_genre_id)+'.xml'
	fx = codecs.open(fname,"w","utf-8")
	fx.write('<?xml version="1.0" ?>\n')
	curr_genre = sa.artist() 
	curr_genre.set_artistName(curr_genreName)
	curr_genre.set_artistId(curr_genre_id)
	#change this to correct sparse matrix manipulations
        tuples =  zip(curr_row.indices,curr_row.data)
        sorted_g = sorted(tuples, key=lambda score: score[1], reverse=True)
        sorted_g = sorted_g[0:100]      
	for pair in sorted_g:
            j = pair[0]
            curr_similar_genre = combinedgenresdictrev[j]
	    curr_similar_artist_id = j
            similar_genre = sa.similarArtists()
	    similar_genre.set_artistName(curr_similar_genre)
	    similar_genre.set_artistId(curr_similar_artist_id)
	    similar_genre.set_cosineDistance(pair[1])
	    curr_genre.add_similarArtists(similar_genre)
	curr_genre.export(fx,0)
	fx.close()
    except Exception as e:
        print e

def createDirectory(directoryName):
    if(not os.path.exists(directoryName)):
	os.mkdir(directoryName)

if __name__ == '__main__':
    t1=datetime.now()
    column_list_genres = []
    rows_list_genres = []
    data_list_genres = []
    col_max_genres = 432
    row_max_genres =0
    createDirectory('simcombinedgenredir')
    cosinesimilaritygenre = []
    #remapped_artist_file_newtest
    row_max_genres,col_max_genres = getmatrixdata_genres('remapped_artist_sample.txt',row_max_genres,col_max_genres)
    #row_max_genres,col_max_genres = getmatrixdata_genres('remapped_artist_file_newtest.txt',row_max_genres,col_max_genres)
   
    
    generateCombinedMatrix()
    t2=datetime.now()
    print 'completed'
    print str(t2-t1)

