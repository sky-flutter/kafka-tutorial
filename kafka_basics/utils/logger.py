import logging
logging.basicConfig(level=logging.DEBUG)

# logging.basicConfig(filename="producer.log",
#                     format="%(asctime)s - %(funcName)s - %(filename)s - %(lineno)d - %(message)s - %(stack_info)s", level=logging.DEBUG)

'''
Logger for common logs
'''
class Logger:

    @classmethod
    def getLogger(self):
        return logging
