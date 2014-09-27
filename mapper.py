#!/usr/bin/env python
# -*- coding: utf-8 -*-

# DBpedia Mapper
# VI: Viktor Maruna

import sys
import rdflib
import StringIO

###############################################################################
def main():

	for line in sys.stdin:
		try:
			line = line.strip()
			

			if line[0]!='#':				
				for stmt in rdflib.Graph().parse(StringIO.StringIO(line), format='nt'): # parse RDF format
					stmt_type = stmt[1].split('#')[-1] # record type (e.g. label)
					stmt_key = stmt[0].split('/')[-1] # record key

					# get record value
					if stmt_type == 'label': # labels
						stmt_value = {stmt_type:str(stmt[2])}				

					print  '%s\t%s' % (stmt_key, stmt_value)

		except:
			pass

if __name__ == '__main__':
    main()
