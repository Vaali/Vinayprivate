# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
from PIL import Image
from random import shuffle
import numpy as np
import pickle
import hickle

from imageFilesTools import getImageData
from config import datasetPath
from config import slicesPath,slicesPathPredict
import h5py
from operator import itemgetter
import shutil

#Creates name of dataset from parameters
def getDatasetName(nbPerGenre, sliceSize):
    name = "{}".format(nbPerGenre)
    name += "_{}".format(sliceSize)
    return name

#Creates or loads dataset if it exists
#Mode = "train" or "test"
def getDataset(nbPerGenre, genres, sliceSize, validationRatio, testRatio, mode):
    print("[+] Dataset name: {}".format(getDatasetName(nbPerGenre,sliceSize)))
    #if not os.path.isfile(datasetPath+"train_X_"+getDatasetName(nbPerGenre, sliceSize)+".p"):
    print("[+] Creating dataset with {} slices of size {} per genre... ⌛�?".format(nbPerGenre,sliceSize))
    return createDatasetFromSlices(nbPerGenre, genres, sliceSize, validationRatio, testRatio) 
    #else:
    #    print("[+] Using existing dataset")
    
    #return loadDataset(nbPerGenre, genres, sliceSize, mode)
        
#Loads dataset
#Mode = "train" or "test"
def loadDataset(nbPerGenre, genres, sliceSize, mode):
    #Load existing
    datasetName = getDatasetName(nbPerGenre, sliceSize)
    if mode == "train":
        print("[+] Loading training and validation datasets... ")
        print( "before loading")
        train_X = hickle.load("{}train_X_{}.p".format(datasetPath,datasetName))
        train_y = hickle.load("{}train_y_{}.p".format(datasetPath,datasetName))
        validation_X = hickle.load("{}validation_X_{}.p".format(datasetPath,datasetName))
        validation_y = hickle.load("{}validation_y_{}.p".format(datasetPath,datasetName))
        
        train_z = hickle.load("{}train_z_{}.p".format(datasetPath,datasetName))
        validation_z = hickle.load("{}validation_z_{}.p".format(datasetPath,datasetName))
        print("    Training and validation datasets loaded! ✅")
        return train_X, train_y, validation_X, validation_y, train_z, validation_z

    else:
        print("[+] Loading testing dataset... ")
        test_X = hickle.load("{}test_X_{}.p".format(datasetPath,datasetName))
        test_y = hickle.load("{}test_y_{}.p".format(datasetPath,datasetName))
        test_z = hickle.load("{}test_z_{}.p".format(datasetPath,datasetName))
        print("    Testing dataset loaded! ✅")
        return test_X, test_y, test_z

#Delete dataset
def deleteDataset():
    try:
        shutil.rmtree(datasetPath)
    except OSError, e:
        print(e)

#Saves dataset
def saveDataset(train_X, train_y, validation_X, validation_y, test_X, test_y, nbPerGenre, genres, sliceSize,train_z,validation_z,test_z):
     #Create path for dataset if not existing
    if not os.path.exists(os.path.dirname(datasetPath)):
        try:
            os.makedirs(os.path.dirname(datasetPath))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    #SaveDataset
    print("[+] Saving dataset... ")
    datasetName = getDatasetName(nbPerGenre, sliceSize)
    hickle.dump(train_X, "{}train_X_{}.p".format(datasetPath,datasetName))
    hickle.dump(train_y, "{}train_y_{}.p".format(datasetPath,datasetName))
    hickle.dump(validation_X,"{}validation_X_{}.p".format(datasetPath,datasetName))
    hickle.dump(validation_y, "{}validation_y_{}.p".format(datasetPath,datasetName))
    hickle.dump(test_X, "{}test_X_{}.p".format(datasetPath,datasetName))
    hickle.dump(test_y, "{}test_y_{}.p".format(datasetPath,datasetName))
    
    hickle.dump(train_z, "{}train_z_{}.p".format(datasetPath,datasetName))
    hickle.dump(validation_z,"{}validation_z_{}.p".format(datasetPath,datasetName))
    hickle.dump(test_z,"{}test_z_{}.p".format(datasetPath,datasetName))
    print("    Dataset saved! ✅💾")

#Creates and save dataset from slices
def createDatasetFromSlices(nbPerGenre, genres, sliceSize, validationRatio, testRatio):
    data = []
    print(genres)
    for genre in genres:
        print("-> Adding {}...".format(genre))
        #Get slices in genre subfolder
        filenames = os.listdir(slicesPath+genre)
        filenames = [filename for filename in filenames if filename.endswith('.png')]
        filenames = filenames[:nbPerGenre]
        #Randomize file selection for this genre
        shuffle(filenames)

        #Add data (X,y)
        for filename in filenames:
            imgData = getImageData(slicesPath+genre+"/"+filename, sliceSize)
            label = [1. if genre == g else 0. for g in genres]
            #print("*** FileName : "+filename.split("_")[0])
            OriginalFileNames = filename.split("_")[0]
            data.append((imgData,label,OriginalFileNames))

    #Shuffle data
    shuffle(data)

    #Extract X and y
    X,y,z = zip(*data)

    #Split data
    validationNb = int(len(X)*validationRatio)
    testNb = int(len(X)*testRatio)
    trainNb = len(X)-(validationNb + testNb)
    #Prepare for Tflearn at the same time
    train_X = np.array(X[:trainNb]).reshape([-1, sliceSize, sliceSize, 1])
    train_y = np.array(y[:trainNb])
    train_z = np.array(z[:trainNb])
    validation_X = np.array(X[trainNb:trainNb+validationNb]).reshape([-1, sliceSize, sliceSize, 1])
    validation_y = np.array(y[trainNb:trainNb+validationNb])
    validation_z = np.array(z[trainNb:trainNb+validationNb])
    test_X = np.array(X[-testNb:]).reshape([-1, sliceSize, sliceSize, 1])
    test_y = np.array(y[-testNb:])
    test_z = np.array(z[-testNb:])
    print("    Dataset created! ✅")
        
    #Save
    #saveDataset(train_X, train_y, validation_X, validation_y, test_X, test_y, nbPerGenre, genres, sliceSize,train_z,validation_z,test_z)

    return train_X, train_y, validation_X, validation_y, test_X, test_y, train_z, test_z, validation_z



def createDatasetFromSlicesPredict(nbPerGenre, genres, sliceSize):
    data = []
    for genre in genres:
        print("-> Adding {}...".format(genre))
        #Get slices in genre subfolder
        filenames = os.listdir(slicesPathPredict+genre)
        filenames = [filename for filename in filenames if filename.endswith('.png')]
	#print(filenames)
        filenames = filenames[:nbPerGenre]
        #Randomize file selection for this genre
        #shuffle(filenames)

        #Add data (X,y)
        for filename in filenames:
	    fileid = int(filename.split("_")[0].split(".")[1]) #bluesboogiewoogie.00056.wav_18.png 	
            imgData = getImageData(slicesPathPredict+genre+"/"+filename, sliceSize)
            label = [1. if genre == g else 0. for g in genres]
            data.append((imgData,label,fileid))


	#Extract X and y
    data.sort(key=lambda tup: tup[2])
    #print(data)		
    X,y,z = zip(*data)
    test_X = np.array(X).reshape([-1, sliceSize, sliceSize, 1])
    test_y = np.array(y)
    test_z = np.array(z)
    #print("......................................................")
    #print(y)
    #print("......................................................")
    return test_X,test_y,test_z
