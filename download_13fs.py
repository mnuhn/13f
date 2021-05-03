#!/usr/bin/env python3

"""
Downloads all 13Fs mentioned in forms/form*.idx
"""

import requests
import sys
from bs4 import BeautifulSoup
from datetime import timedelta, date
import urllib.request
import ifcfg
import time
import glob
import re
from tqdm import tqdm

from requests_toolbelt.adapters.source import SourceAddressAdapter


def get_interface_ip(interface="tun0"):
  interfaces = ifcfg.interfaces()
  if interfaces is None:
    return None
  if not interface in interfaces:
    return None
  if not 'inet4' in interfaces[interface]:
    return None
  if len(interfaces[interface]['inet4']) == 0:
    return None
  return interfaces[interface]['inet4'][0]


def download(url, source_ip):
  if source_ip is None:
    return None

  s = requests.Session()
  s.mount('http://', SourceAddressAdapter(source_ip))
  s.mount('https://', SourceAddressAdapter(source_ip))
  s.headers.update({
      'User-Agent':
          'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
  })

  response = s.get(url)

  if response.status_code != 200:
    return None

  if len(response.text) == 0:
    return None

  if response.apparent_encoding is None:
    return None

  response.encoding = response.apparent_encoding

  if response.text is None:
    return None

  return response.text


def read_idx(fn):
  result = []

  started = False
  f = open(fn)
  for l in f:
    if "--------------------------------" in l:
      started = True
      continue

    if not started:
      continue

    fields = l.strip().split()

    form_type = fields[0]
    file_name = fields[-1]

    result.append((form_type, file_name))

  f.close()

  return result


files = []

for idx_filename in glob.glob('./forms/form*.idx'):
  for filing_form_type, filing_filename in read_idx(idx_filename):
    if filing_form_type != "13F-HR":
      continue
    files.append((idx_filename, filing_filename))

pbar = tqdm(files)

for idx_filename, filing_filename in tqdm(files):
  source_ip = get_interface_ip()
  output_filename = "data/" + filing_filename.split("/")[-1]
  url = "https://www.sec.gov/Archives/" + filing_filename
  short_url = "sec.gov/Archives/" + filing_filename
  short_idx_filename = idx_filename.replace("./forms/", "")

  pbar.set_description("%s %s %s" %(short_idx_filename, short_url, source_ip))

  data = download(url, source_ip)

  if data is None:
    print("cannot download", url)
    continue

  f = open(output_filename, "w")
  f.write(data)
  f.close()

  time.sleep(0.25)
