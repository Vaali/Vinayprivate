import scipy.sparse as sp
from scipy.sparse import hstack, vstack
import numpy as np
import codecs
from numpy.lib.stride_tricks import as_strided
import similar_artists_api as sa
import os
import sys
from multiprocessing import Pool
import logging
import logging.handlers
import loggingmodule
from datetime import datetime, date, timedelta

reload(sys)
sys.setdefaultencoding('utf8')


def getmatrixdata(filename,col_max,row_max):
	fileopen = codecs.open(filename,"r","utf-8")
	lines = fileopen.readlines()
        lines = filter(lambda x: x.replace('\n','') != '',lines)
	lines = map(lambda x:x.replace('\n',''),lines)
	print len(lines)
	for line in lines:
		if(line != ''):
			words = line.split(':;')
			curr_row = int(words[1])
                        curr_col = int(words[3])
			rows_list.append(curr_row)
			if(curr_row not in artists_map):
				artists_map[curr_row] = [words[0],words[5],words[6],words[7]]
                        if(curr_col not in genres_map):
                            genres_map[curr_col] = words[2]
			if(row_max < curr_row):
				row_max = curr_row
			if(col_max < int(words[3])):
				col_max = int(words[3])
                        #print col_max
			column_list.append(int(words[3]))
			data_list.append(float(words[4]))
        return row_max,col_max

def sum1(X,v):
    rows, cols = X.shape
    row_start_stop = as_strided(X.indptr, shape=(rows, 2),
                            strides=2*X.indptr.strides)
    for row, (start, stop) in enumerate(row_start_stop):
        data = X.data[start:stop]
        data -= v[row]

def corr(data1, data2):
    "data1 & data2 should be numpy arrays."
    mean1 = data1.mean()
    mean2 = data2.mean()
    std1 = data1.std()
    std2 = data2.std()
    corr = ((data1*data2).mean()-mean1*mean2)/(std1*std2)
    return corr

	
def corrcoef_csr(x, axis=0):
    '''correlation matrix, return type ndarray'''
    covx = cov_csr(x, axis=axis)
    stdx = np.sqrt(np.diag(covx))[np.newaxis,:]
    return covx/(stdx.T * stdx)

def cov_csr(x, axis=0):
    '''return covariance matrix, assumes column variable
    return type ndarray'''
    meanx = x.sum(axis=axis)/float(x.shape[axis])
    if axis == 0:
        return np.array((x.T*x)/x.shape[axis] - meanx.T*meanx)
    else:
        return np.array((x*x.T)/x.shape[axis] - meanx*meanx.T)


'''(x,y,z)=sp.find(temp5)
countings=np.bincount(x)
sums=np.bincount(x,weights=z)
averages=sums/col_max
sum1(temp5, averages)
print temp5.size
#temp5 = temp1 - averages[:,[0,0,0]]
row_sums = ((temp5.multiply(temp5)).sum(axis=1))

rows_sums_sqrt = np.array(np.sqrt(row_sums))[:,0]

row_indices, col_indices = temp5.nonzero()

temp5.data /= rows_sums_sqrt[row_indices]
temp6 = temp5.transpose()
#print temp1
#print temp2
temp8 = temp5*temp6
print temp8'''
#change this to correct sparse matrix manipulations

#print temp7.todense()

'''print temp1.shape
colmat = temp1.tocsc()
#print colmat.todense()
indptr = colmat.indptr
colmat.indptr = (colmat.indptr + 1) % colmat.shape[0]
print colmat.shape
print indptr
#print colmat.todense()
#print temp1[2,:]'''
'''print row[2]
print col[2]
print data[2]

v1 = temp1.data
r1 = temp1.indptr
c1 = temp1.indices

print r1[2]
print c1[2]
print v1[2]'''

#print artists_map

def similarartist((artist_index,sim_mat_index)):
    try:
        #print artist_index
	curr_artistName = artists_map[artist_index][0]
	curr_artist_id = int(artists_map[artist_index][3])
	curr_artist_popularity = int(artists_map[artist_index][1])
	curr_artist_year = int(artists_map[artist_index][2])
	curr_artist = sa.artist() 
	curr_artist.set_artistName(curr_artistName)
	curr_artist.set_artistId(curr_artist_id)
        curr_artist.set_artistPopularityAll(curr_artist_popularity)
	curr_artist.set_earliestDate(curr_artist_year)
	#change this to correct sparse matrix manipulations
        #print 'here'
        #print i
        #print row_indices[i]
        written = 0
        currentrow = cosinesimilarityartist[sim_mat_index,:].toarray()
	for j in row_indices[sim_mat_index]:
            if(currentrow[0][j] > 0.75):
                written = 1
		curr_similar_artist = artists_map[j][0]
		curr_similar_artist_id = int(artists_map[j][3])
		curr_similar_artist_popularity = int(artists_map[j][1])
		curr_similar_artist_year = int(artists_map[j][2])
		
		similar_artists = sa.similarArtists()
		similar_artists.set_artistName(curr_similar_artist)
		similar_artists.set_artistId(curr_similar_artist_id)
		similar_artists.set_artistPopularityAll(curr_similar_artist_popularity)
		similar_artists.set_earliestDate(curr_similar_artist_year)
		similar_artists.set_cosineDistance(currentrow[0][j])
		#similar_artists.set_euclideanDistance(euclideandistanceartist[i][j])
		#similar_artists.set_pearsonDistance(pearsondistanceartist[i,:][j])
                curr_artist.add_similarArtists(similar_artists)
        if(written == 1):
            fname = 'simartistdir/' + str(curr_artist_id)+'.xml'
	    fx = codecs.open(fname,"w","utf-8")
	    fx.write('<?xml version="1.0" ?>\n')
	    curr_artist.export(fx,0)
	    fx.close()
    except Exception as e:
	logger_matrix.exception(e)


def similargenre(i):
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
	for j in genres_map:
            if(cosinesimilaritygenre[i,:].toarray()[0][j] > 0):
		curr_similar_genre = genres_map[j]
		curr_similar_artist_id = j
		#curr_similar_artist_popularity = int(genres_map[j][1])
		#curr_similar_artist_year = int(genres_map[j][2])
		
		similar_genre = sa.similarArtists()
		similar_genre.set_artistName(curr_similar_genre)
		similar_genre.set_artistId(curr_similar_artist_id)
		#similar_genre.set_artistPopularityAll(curr_similar_artist_popularity)
		#similar_genre.set_earliestDate(curr_similar_artist_year)
		similar_genre.set_cosineDistance(cosinesimilaritygenre[i,:].toarray()[0][j])
		#similar_genre.set_euclideanDistance(euclideandistancegenre[i][j])
		#similar_genre.set_pearsonDistance(pearsondistancegenre[i,:][j])		
		curr_genre.add_similarArtists(similar_genre)	
	curr_genre.export(fx,0)
	fx.close()
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

if __name__ == '__main__':
    logger_matrix = loggingmodule.initialize_logger('matrixoperations.log')
    t1=datetime.now()
    try:
        methodName1 = 'Cosine similarity'
        methodName2 = 'Euclidean distance'
        methodName3 = 'Pearson distance'
        column_list = []
        rows_list = []
        data_list = []
        col_max = 0
        row_max = 0
        earlier_year = 2050
        artists_map = {}
        genres_map = {}
        if(not os.path.exists('simartistdir')):
	    os.mkdir('simartistdir')
        if(not os.path.exists('simgenredir')):
	    os.mkdir('simgenredir')

        np.set_printoptions(suppress=True)
        row_max,col_max = getmatrixdata('remapped_artist_file.txt',col_max,row_max)
        print row_max
        print col_max
        row  = np.array(rows_list)
        col  = np.array(column_list)
        data = np.array(data_list)
        artistgenrematrix = sp.coo_matrix((data, (row, col)), shape=(row_max+1, col_max+1))
        tempA1 = artistgenrematrix.tocsr()
        #print temp1
        
        #Cosine similarity
        print tempA1.shape
        #calculating the row sums 
        row_sums = ((tempA1.multiply(tempA1)).sum(axis=1))
        #calculating the sqrt of the sums
        rows_sums_sqrt = np.array(np.sqrt(row_sums))[:,0]
        #divide and get the norms
        row_indices, col_indices = tempA1.nonzero()
        tempA1.data /= rows_sums_sqrt[row_indices]
        
        tempA2 = tempA1.transpose()
        
        #change this to correct sparse matrix manipulations

        #break the matrix into peices

        block_indices = range(100,tempA1.shape[0],100)
        #function returns the blocks of the main matrix        
        split_mat = split_sparse(tempA1,block_indices,[])

        index = 0
        block_indices = [0]+ block_indices + [row_max]
        #foreach block returned calculate the cosine similarity 
        
        for split in split_mat:
            #split[0] because the columns are not split
            cosinesimilarityartist = split[0]*tempA2
            #print cosinesimilarityartist.shape
            #get the 
            row_indices = np.split(cosinesimilarityartist.indices, cosinesimilarityartist.indptr[1:-1])
            logger_matrix.exception('writing the artists files')
            sim_matrix_arguments = zip(range(block_indices[index],block_indices[index+1]),range(block_indices[index+1]-block_indices[index]))
            #print sim_matrix_arguments
            p =Pool(processes=int(100))
	    p.map(similarartist,sim_matrix_arguments)
	    p.close()
	    p.join()
            index = index+1


        print 'cosine distance'
        #print temp3.todense()
        #eucliden distance
        size = cosinesimilarityartist.shape
        '''print 'euclidean distance'
        tempA4 = 2*(np.ones(size) - cosinesimilarityartist)

        #temp4 = (np.sqrt(2*(np.ones(size))) - np.sqrt(np.absolute(np.around(temp4,decimals=8))))/1.41421356
        euclideandistanceartist = np.sqrt(np.absolute(np.around(tempA4,decimals=8)))
        #change this to correct sparse matrix manipulations
        #print temp4
        #pearson distance
        print 'pearson distance'
        tempA5 = artistgenrematrix.tocsr()
        tempA5 = np.around(tempA5,decimals=8)
        xmat = tempA5.todense()[0,:].tolist()
        #print (xmat)
        ymat =  tempA5.todense()[1,:].tolist()
        #print (ymat)
        pearsondistanceartist = corrcoef_csr(tempA5,1)
        #print temp7'''

        row_indices = np.split(cosinesimilarityartist.indices, cosinesimilarityartist.indptr[1:-1])
        #print row_indices[0]
        '''logger_matrix.exception('writing the artists files')
        p =Pool(processes=int(100))
	p.map(similarartist,artists_map.keys())
	p.close()
	p.join()'''

        #genres

        '''
        tempG1 = artistgenrematrix.tocsr()
        tempG1 = tempG1.transpose()
        print tempG1.todense()
        row_sums_genres = ((tempG1.multiply(tempG1)).sum(axis=1))
        
        rows_sums_sqrt_genres = np.array(np.sqrt(row_sums_genres))[:,0]
        print rows_sums_sqrt_genres
        row_indices, col_indices = tempG1.nonzero()
        print row_indices
        print tempG1.data[2] 
        tempG1.data /= rows_sums_sqrt_genres[col_indices]
        print tempG1.todense()
        tempG2 = tempG1.transpose()
        cosinesimilaritygenre = tempG1*tempG2
        print 'cosine distance' '''
        '''size = cosinesimilaritygenre.shape
        tempG4 = 2*(np.ones(size) - cosinesimilaritygenre)
        euclideandistancegenre = np.sqrt(np.absolute(np.around(tempG4,decimals=8)))
        print 'euclidean distance'
        
        tempG5 = artistgenrematrix.tocsr()
        tempG5 = tempG5.transpose()

        tempG5 = np.around(tempG5,decimals=8)
        xmat = tempG5.todense()[0,:].tolist()
        #print (xmat)
        ymat =  tempG5.todense()[1,:].tolist()
        #print (ymat)
        pearsondistancegenre = corrcoef_csr(tempG5,1)
        print pearsondistancegenre.shape '''
        '''logger_matrix.exception('writing the genre files')
        a =Pool(processes=int(100))
	a.map(similarartist,artists_map.keys())
	a.close()
	a.join()

        logger_matrix.exception('writing the  files')
        g =Pool(processes=int(100))
	g.map(similargenre,genres_map.keys())
	g.close()
	g.join()'''

        t2=datetime.now()
        print (t2-t1)
    except Exception as e:
	logger_matrix.exception(e)
	
		




