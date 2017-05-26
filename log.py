# -*- coding: utf-8 -*-
"""
Logging module, to be universal to all modules
"""


import sys
import logging
import logging.handlers


def setup_custom_logger(name):
	LOG_FILENAME = 'logs/LISTS.log'
	formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
	handler = logging.handlers.RotatingFileHandler(
										LOG_FILENAME, maxBytes=1024*1024, backupCount=10)


	#~ handler = logging.StreamHandler()
	handler.setFormatter(formatter)

	logger = logging.getLogger(name)
	logger.setLevel(logging.INFO)
	logger.addHandler(handler)
	return logger
