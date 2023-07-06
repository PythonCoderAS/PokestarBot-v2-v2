from os import getenv
import logging

if getenv("DEBUG"):
    logging.basicConfig(level=logging.DEBUG)
