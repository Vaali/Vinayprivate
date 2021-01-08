import logging
import logging.handlers
import sys
import os


reload(sys)
sys.setdefaultencoding('utf8')

if(not os.path.exists('logs')):
    os.mkdir('logs',0755)
#class gramusik_logger():
def initialize_logger(loggername,logfileName,time= True):
        
        if(time == True):
            formatter = logging.Formatter('%(asctime)s %(message)s')
        else:
            formatter = logging.Formatter('%(message)s')
        currlogger = logging.getLogger(loggername)
        hdlr = logging.handlers.RotatingFileHandler(
            'logs/'+logfileName, maxBytes=10240*1024*1024, backupCount=500)
        hdlr.setFormatter(formatter)
        if(os.path.isfile('logs/'+logfileName)):
            hdlr.doRollover()
        currlogger.addHandler(hdlr)
        return currlogger

def initialize_logger1(loggername,logfilename):
        formatter = logging.Formatter('%(asctime)s %(message)s')
        currlogger = logging.getLogger(loggername)
        hdlr = logging.handlers.RotatingFileHandler(
            'logs/'+logfilename, maxBytes=10240*1024*1024, backupCount=500)
        hdlr.setFormatter(formatter)
        if(os.path.isfile('logs/'+logfilename)):
            hdlr.doRollover()
        currlogger.addHandler(hdlr)
        return currlogger

def initialize_logger_withoutrolling(loggername,logfilename):
        formatter = logging.Formatter('%(asctime)s %(message)s')
        currlogger = logging.getLogger(loggername)
        hdlr = logging.FileHandler(
            'logs/'+logfilename, mode='a')
        hdlr.setFormatter(formatter)
        currlogger.addHandler(hdlr)
        return currlogger

def initialize_logger_stdout(loggername):
        currlogger = logging.getLogger(loggername)
        handler = logging.StreamHandler(sys.stdout)
        currlogger.addHandler(handler)
        return currlogger
