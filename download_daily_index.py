#!/usr/bin/env python3

"""
  Downloads 13f index files from sec.gov in the given date range. Dates have to
  be in format YYYY-MM-DD.
"""

import argparse
import requests
import sys
from bs4 import BeautifulSoup
from datetime import timedelta, date, datetime
import urllib.request
import ifcfg
import time
from tqdm import tqdm

from requests_toolbelt.adapters.source import SourceAddressAdapter


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("start_date", help="start date")
parser.add_argument("end_date", help="end date")
args = parser.parse_args()



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

  # print("using source interface", source_ip)
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


def get_url(date):
  quarter = (date.month - 1) // 3 + 1
  return "https://www.sec.gov/Archives/edgar/daily-index/%d/QTR%d/form.%04d%02d%02d.idx" % (
      date.year, quarter, date.year, date.month, date.day)


def get_fn(date):
  return "./forms/form.%04d%02d%02d.idx" % (date.year, date.month, date.day)


def daterange(start_date, end_date):
  result = []
  for n in range(int((end_date - start_date).days)):
    result.append(start_date + timedelta(n))
  return result


start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
date_range = daterange(start_date, end_date)

pbar = tqdm(date_range)

for date in pbar:
  url = get_url(date)
  source_ip = get_interface_ip()
  pbar.set_description("Downloading " + url + " with ip " + source_ip)

  time.sleep(0.5)

  data = download(url, source_ip)

  if data is None:
    print("could not download", url)
    continue

  fn = get_fn(date)
  f = open(fn, "w")
  f.write(data)
  f.close()
