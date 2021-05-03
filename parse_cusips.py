#!/usr/bin/env python3
"""
  Reads a 13f list in text format (converted from pdf with pdftotext --layout),
  parses it and adds it to the given sqlite db.
"""

import re
import argparse
import sqlite3
import sys
import os

CUSIP_REGEX = "([A-Z0-9]{6}) ([0-9]{2}) ([0-9]) [*]?"
SPACE_REGEX = " +"
COMPANY_REGEX = "(([^ ]+ ?)+)"
TYPE_REGEX = "(([^ ]+ ?)+)"
DELADD_REGEX = "( +(DELETED|ADDED))?"

REGEX = CUSIP_REGEX + SPACE_REGEX + COMPANY_REGEX + SPACE_REGEX + TYPE_REGEX + DELADD_REGEX

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("input", help="input filename")
parser.add_argument("sqlite", help="output filename")
args = parser.parse_args()

conn = sqlite3.connect(args.sqlite, check_same_thread=False)

def init():
  c = conn.cursor()
  try:
    c.execute('''
    CREATE TABLE cusips (
      id INTEGER PRIMARY KEY,
      cusip TEXT,
      company TEXT,
      type TEXT
    )''')
  except Exception as e:
    print("table existed:", e)


def add_cusip(cusip, company, entry_type):
  c = conn.cursor()
  c.execute(
        '''INSERT OR REPLACE INTO cusips(cusip,company,type)
                 VALUES (?,?,?)''', (cusip, company, entry_type))
  return c.lastrowid

if not os.path.isfile(args.input):
  print("input file does not exist")
  sys.exit(1)



init()

with open(args.input, "r") as infile:
  for l in infile:
    l = l.strip()
    m = re.match(REGEX, l)
    if not m:
      print("error parsing", l)
      continue

    cusip = (m.group(1) + m.group(2) + m.group(3)).strip()
    company = (m.group(4)).strip()
    entry_type = (m.group(6)).strip()

    if m.group(8) is None:
      update = "KEEP"
    else:
      update = update.strip()

    if update == "DELETE":
      continue

    add_cusip(cusip, company, entry_type)

conn.commit()
