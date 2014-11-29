#!/usr/bin/env python

from pyspark import SparkContext
from lxml import etree, html
from collections import Counter
import time

dates = ['2014%02d'%x for x in range(1,12)]

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
		except:
			print 'Retry %d for url %s' % (i, url)
			time.sleep(0.1)
			continue
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
	return sum(map(lambda x:x[1], devSorted))

if __name__ == "__main__":
	sc = SparkContext(appName="ListCrawler")
	links = listindex()
	distLinks = sc.parallelize(links)

	total = distLinks.map(processProject).reduce(lambda a, b: a + b)
	print "Total participants: %d" % (total)
	sc.stop()
		
