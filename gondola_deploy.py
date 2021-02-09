#!/usr/bin/env python

import sys, getopt
from pathlib import Path
from core import properties_reader as prop, deploy_changes

def main(argv):
    help = "-t, --tgt      Target Database Name\n" \
           "-c, --clone    "