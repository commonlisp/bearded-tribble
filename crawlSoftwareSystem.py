#!/usr/bin/env python

from lxml import etree, html
from collections import Counter
import time
import urllib2
import sys
import datetime

base_url = "http://awards.acm.org/software_system/year.cfm"

def procWinner(descr):
	project = descr[0].text_content()
	members = []
	for link in descr.cssselect('dd a'):
		members.append(link.text.strip())
	return project, members

def sanitize(member_name):
	return member_name.encode("ascii", 'ignore').replace(' ', '/')

doc = html.parse(base_url)
rt = doc.getroot()
descr = rt.cssselect('dl')
projectMembers = map(procWinner, descr)
