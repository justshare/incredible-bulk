# -*- coding: utf-8 -*-
from __future__ import print_function
import requests, re, os, sys, time, pdb, csv
from flask import json
from twitter_text import TwitterText
from clint import arguments
from clint.textui import puts, indent, colored

SHORTEN = 'shorten?url=%s'
SCHEDULED_POST = 'profile/%s/post/scheduledmessage'

INPUT_DIR = '.'

def prefix_if_needed(str):
  """
  Add an http prefix for bare urls such as www.levitum.in
  """
  if 'http' in str or 'https' in str:
    return str
  else:
    return 'http://%s' % str

def shorten_url(url_in):
  """
  Wrapper for the link shortener, currently fails silently returning None
  """
  url_in = prefix_if_needed(url_in)
  shortened_url = None
  puts(colored.yellow('INFO: URL to be shortened is %s' % url_in))
  url = ROOT + SHORTEN % url_in
  r = requests.get(url)
  if r.status_code == requests.codes.ok:
    obj = r.json()
    shortened_url = obj.get('url')
    if obj is not None and shortened_url is not None:
      with indent(4, quote=' >'):
        puts(colored.cyan('shortened URL is %s' % shortened_url))
    else:
      print("ERROR: Link Shortening Failed - ", r.content, file=sys.stderr)
  else:
    print("ERROR: Link Shortening Failed - ", r.content, file=sys.stderr)
  return shortened_url

def generate_schedule(num_posts, post_interval):
  """
  Generates a time series based on the # of posts and the post_interval passed in.
  """
  scheduled_times = []
  now = int(time.time())
  [scheduled_times.append(now+post_interval*iter*60) for iter in range(num_posts)]
  return scheduled_times

def read_csv(filename):
  """
  Reads the CSV file, pulls data from the first column
  """
  with open(os.path.join(INPUT_DIR,filename)) as infile:
    reader = csv.reader(infile)
    return(tuple(dict(message=x[0]) for x in reader if len(x[0])>0))
  return []

def process_links(msg_list):
  """
  For each message, extract URLs, shorten them, add link meta data
  """
  data = []
  for msg in msg_list:
    message = msg.get('message')
    tt = TwitterText(message)
    ex = tt.extractor
    orig_urls = ex.extract_urls()
    short_urls = ex.extract_urls(shorten_url)
    link = short_urls[0] if len(short_urls) > 0 else None
    for a,b in zip(orig_urls, short_urls):
      message = message.decode("utf-8").replace(a, b).encode("utf-8")
    obj = dict(message=message, meta=dict(link=link))
    data.append(obj)
  return data

def post_scheduled_message(schedule):
  """
  In the absence of a bulk scheduling API, make individual calls to schedule each post
  """
  for post in schedule:
    url = ROOT + SCHEDULED_POST % PROFILE
    headers = {'content-type': 'application/json'}
    r = requests.post(url, data=json.dumps(post), headers=headers)
    if r.status_code == requests.codes.ok:
      puts(colored.yellow('INFO: Successfully scheduled: %s' % post.get('message')))
    else:
      print("ERROR: Scheduling Post Failed - ", r.content, file=sys.stderr)

def bulk_just_share():
  csv_data = read_csv(CSV_FILE)
  data = process_links(csv_data)
  times = generate_schedule(len(data), INTERVAL*15)
  schedule = []
  for obj, time in zip(data, times):
    obj['time'] = time
    schedule.append(obj)

  puts('INFO: Post schedule that has been generated is %s' % schedule)
  puts(colored.yellow('INFO: Dispatching batch to be scheduled'))

  post_scheduled_message(schedule)

if __name__ == "__main__":
  args = arguments.Args()

  if len(args) < 1:
    print("Usage: python bulkshr.py config.json", file=sys.stderr)
    exit(1)

  with open(args.get(0)) as data_file:
      data = json.load(data_file)

  with indent(4, quote=' <'):
    puts(colored.cyan('imported config: %s' % json.dumps(data)))

  PROFILE = data.get('PROFILE')
  CSV_FILE = data.get('CSV_FILE')
  ROOT = data.get('ROOT')
  INTERVAL = data.get('INTERVAL')

  bulk_just_share()
