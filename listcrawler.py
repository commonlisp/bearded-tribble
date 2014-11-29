#!/usr/bin/env python

import lxml.html
from lxml import etree
from collections import Counter

dates = ['2014%02d'%x for x in range(1,12)]

baseurl = "http://mail-archives.apache.org/mod_mbox/"
commitList = "mesos-commits/"
discussList = "mesos-dev/"

def cleanName(t):
	if t.endswith('(JIRA)'):
		return t[:-7]
	return t

def scanUrl(listname, date, n):
	url = baseurl + listname +date+".mbox/date?"+str(n)
	doc = lxml.html.parse(url)
	r = doc.getroot()
	subjects = map(lambda x:x[0].text, r.find_class('subject'))
	authors = map(lambda x:cleanName(x.text), r.find_class('author'))
	return subjects, authors

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
	doc = lxml.html.parse(baseurl)
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

links = listindex()
if len(links) < 4:
	print "Cannot find links"
	exit(0)
maxProjects = 4

for i in range(maxProjects):
	dev = links[i]['dev']
	discussParticipants = counts(participation(dev))
	commitParticipants = counts(participation(links[i]['commit']))
	print "Participation (discuss " + dev + "):\n" + str(sorted(discussParticipants, key=lambda x:x[0]))
	print "Participation (commit):\n" + str(sorted(commitParticipants, key=lambda x:x[0]))
