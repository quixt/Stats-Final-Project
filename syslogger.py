# **********************************************
# Simple logging for the main program to debug
# **********************************************

import sys

class syslogger():
    def __init__(self):
        self.num = 0
        sys.excepthook = self.errorCatch
    # args:
    # - message (Str): The message to print for the logging
    # - prog (Bool): Whether or not the action is in progress
    def log(self, message, prog=False):
        print(f"[{self.num}] {message}{'...' if prog else ''}")
        self.num += 1
    
    def errorCatch(self, exc_type, exc_value, exc_traceback):
        print(f"Error between [{self.num-1}] and [{self.num}]: {exc_type.__name__} | {exc_value}")