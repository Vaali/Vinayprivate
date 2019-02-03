import sys
import codecs
import os

reload(sys)
sys.setdefaultencoding('utf8')

def is_songname_same_artistname(songname,artistname):
    return (songname.lower() == artistname.lower())