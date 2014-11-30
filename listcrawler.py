#!/usr/bin/env python

from pyspark import SparkContext
from lxml import etree, html
from collections import Counter
import time
import urllib2
import sys
import datetime

today = datetime.date.today()
dates = ['%d%02d'%(today.year, month) for month in range(1,today.month)]

baseurl = "http://mail-archives.apache.org/mod_mbox/"
maxTries = 5

def cleanName(t):
	if t.endswith('(JIRA)'):
		return t[:-7]
	return t

def scanUrl(listname, date, n):
	url = baseurl + listname +date+".mbox/date?"+str(n)
	for i in range(maxTries):
		try:
			doc = html.parse(url)
			r = doc.getroot()
			subjects = map(lambda x:x[0].text, r.find_class('subject'))
			authors = map(lambda x:cleanName(x.text), r.find_class('author'))
			return subjects, authors
		except IOError as e:
			print 'Retry %d for url %s: exception %s' % (i, url, str(e))
			time.sleep(0.1)
			continue
		except Exception as e:
			print 'For url %s, exception %s' % (url, str(e))
			break
	return [], []

def participation(listname):
	c = {}
	for d in dates:
		subjects = []
		authors = []
		n = 0
		while True:
			s, a = scanUrl(listname, d, n)
			if s == []:
				break
			n += 1
			subjects.extend(s)
			authors.extend(a)
		c[d] = Counter(authors)
	return c

def counts(dateSeries):
	return map(lambda itm: (itm[0], len(itm[1].items())), dateSeries.items())

def listindex():
	doc = html.parse(baseurl)
	r = doc.getroot()
	nodes = r.cssselect('td ul li')
	index = []
	for n in nodes:
		links = n.cssselect('ul li a[href]')
		if links is None:
			continue
		cmt = filter(lambda elem: elem.text == "commits", links)
		dev = filter(lambda elem: elem.text == "dev", links)
		if len(cmt) > 0 and len(dev) > 0:
			index.append({'commit': cmt[0].get('href'), 'dev': dev[0].get('href')})
	return index

def processProject(linkPair):
	dev = linkPair['dev']
	discussParticipants = counts(participation(dev))
	commitParticipants = counts(participation(linkPair['commit']))
	devSorted = sorted(discussParticipants, key=lambda x:x[0])
	commitSorted = sorted(commitParticipants, key=lambda x:x[0])
	print "Participation (discuss " + dev + "):\n" + str(devSorted)
	print "Participation (commit):\n" + str(commitSorted)
	return {'dev': devSorted, 'commit': commitSorted }

if __name__ == "__main__":
	sc = SparkContext(appName="ListCrawler")
	if len(sys.argv) < 2:
		print "Usage: %s [hdfs namenode]" % (sys.argv[0])
		exit(1)
	namenode_host = sys.argv[1]
	links = listindex()
	distLinks = sc.parallelize(links)

	distLinks.map(processProject).saveAsTextFile('hdfs://' + namenode_host + '/links.txt')
	sc.stop()
		
