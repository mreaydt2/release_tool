#!/usr/bin/env python

import sys, getopt
from pathlib import Path
from core import properties as prop, deploy

def main(argv):
    help = "-t, --tgt      Target Database Name\n" \
           "-c, --clone    "