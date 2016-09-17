import logging
import logging.handlers
import sys


reload(sys)
sys.setdefaultencoding('utf8')
def initialize_logger(logfileName,time= True):
    if(time == True):
        formatter = logging.Formatter('%(asctime)s %(message)s')
    else:
        formatter = logging.Formatter('%(message)s')
    currlogger = logging.getLogger('simple_logger')
    hdlr = logging.handlers.RotatingFileHandler(
            logfileName, maxBytes=10240*1024*1024, backupCount=500)
    hdlr.setFormatter(formatter)
    currlogger.addHandler(hdlr)
    return currlogger

def initialize_logger1(logfilename):
    formatter = logging.Formatter('%(asctime)s %(message)s')
    currlogger = logging.getLogger('simple_logger_2')
    hdlr = logging.handlers.RotatingFileHandler(
            logfilename, maxBytes=10240*1024*1024, backupCount=500)
    hdlr.setFormatter(formatter)
    currlogger.addHandler(hdlr)


