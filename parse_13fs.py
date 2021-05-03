#!/usr/bin/env python3

import sys
import glob
import sqlite3
from tqdm import tqdm

import lxml
import cchardet
from bs4 import BeautifulSoup


conn = sqlite3.connect('filings.db', check_same_thread=False)

def init():
  c = conn.cursor()
  try:
    c.execute('''
    CREATE TABLE filings (
      id INTEGER PRIMARY KEY,
      cik INTEGER,
      submission_type TEXT,
      period TEXT
    )''')
  except Exception as e:
    print("table existed:", e)

  try:
    c.execute('''
    CREATE TABLE entries (
      id INTEGER PRIMARY KEY,
      filing_id INTEGER,
      cusip TEXT,
      count INTEGER,
      count_type TEXT,
      FOREIGN KEY(filing_id) REFERENCES filings(id)
    )''')
  except Exception as e:
    print("table existed:", e)

def add_filing(cik, submission_type, period):
  c = conn.cursor()
  c.execute(
        '''INSERT OR REPLACE INTO filings(cik,submission_type,period)
                 VALUES (?,?,?)''', (cik, submission_type, period))
  return c.lastrowid

def add_entry(filing_id, cusip, count, count_type):
  c = conn.cursor()
  c.execute(
        '''INSERT OR REPLACE INTO entries(filing_id, cusip, count, count_type)
                 VALUES (?,?,?,?)''', (filing_id, cusip, count, count_type))



def maybe_get_text(x):
  if x is None:
    return None
  return x.text

def extract_fields(filename):
  f = open(filename)
  data = f.read()
  f.close()

  metadata = None
  table = None

  for x in data.split("<XML>"):
    x = x.split("</XML>")[0]
    x = x.strip()
    if "informationTable" in x:
      table = x
    elif "edgarSubmission" in x:
      metadata = x

  submission_type = None
  period = None
  cik = None

  if metadata != None:
    soup = BeautifulSoup(metadata, "lxml")

    submission_type = maybe_get_text(soup.find("submissiontype"))
    period = maybe_get_text(soup.find("periodofreport"))
    cik = maybe_get_text(soup.find("cik"))

  else:
    print("no metadata")

  result = []

  if table != None:
    soup = BeautifulSoup(table, "lxml")

    for x in soup.find_all("infotable"):
      cusip = maybe_get_text(x.find("cusip"))
      count = maybe_get_text(x.find("sshprnamt"))
      count_type = maybe_get_text(x.find("sshprnamttype"))
      #title = maybe_get_text(x.find("titleofclass"))
      #issuer = maybe_get_text(x.find("nameofissuer"))

      result.append((cusip, count, count_type))
  else:
    print("no table")

  return submission_type, period, cik, result

init()


pbar = tqdm(glob.glob('./data/*.txt'))
for filename in pbar:
  pbar.set_description("Reading %s" % filename)
  submission_type, period, cik, results = extract_fields(filename)

  if submission_type is None or period is None or cik is None:
    print("broken data")
    continue

  filing_id = add_filing(cik, submission_type, period)

  for cusip, count, count_type in results:
    if cusip is None or count is None or count_type is None:
      print("skipping broken entry")
      continue
    add_entry(filing_id, cusip, count, count_type)

  conn.commit()
