#!/usr/bin/env python
# -*- coding: utf-8 -*-

# DBpedia Mapper
# VI: Viktor Maruna

import sys
import rdflib
import StringIO

reload(sys)
sys.setdefaultencoding('utf-8')

# get record from rdf
def record_parser(text):
	if '#type' in text:
		return 'type' # type
	elif 'terms/subject' in text:
		return 'category' # category
	elif 'property/wikiPageUsesTemplate' in text:
		return "template" # template
	elif 'ontology/wikiPageDisambiguates' in text:
		return 'disambiguates' # disambiguates
	else:
		return 'property' # property

# get type
def type_parser(text):
	if 'http://dbpedia.org/ontology' in text:
		return text.split('/')[-1] 
	else:
		return None	

# get category
def category_parser(text):
	return text.split(':')[-1]

# get template
def template_parser(text):
	return text.split(':')[-1]

# get disambiguates
def disambiguates_parser(text):
	return text.split('/')[-1] 

# get propery
def property_parser(text):
	return text.split('/')[-1] 

###############################################################################
def main():

	for line in sys.stdin:
		try:
			line = line.strip()
		
			if line[0]!='#': # first line				

				for stmt in rdflib.Graph().parse(StringIO.StringIO(line), format="nt"): # parse RDF format
					

					stmt_type = record_parser(stmt[1].encode('utf-8', 'ignore')) # record type
					stmt_key = stmt[0].encode('utf-8', 'ignore').split('/')[-1] # record key

					# value
					if stmt_type == 'type':
						stmt_value = {stmt_type:type_parser(stmt[2])}
					elif stmt_type == 'category':
						stmt_value = {stmt_type:category_parser(stmt[2])}
					elif stmt_type == 'template':
						stmt_value = {stmt_type:template_parser(stmt[2])}
					elif stmt_type == 'disambiguates':
						stmt_value = {stmt_type:disambiguates_parser(stmt[2])}
					else:
						stmt_value = {stmt_type:property_parser(stmt[1])}

					if not None in stmt_value.values():
						print  '%s\t%s' % (stmt_key, stmt_value) # mapper output

		except Exception, err:
			sys.stderr.write('Mapper ERROR: %s\n' % str(err))

if __name__ == '__main__':
    main()
