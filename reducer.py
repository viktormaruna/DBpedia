#!/usr/bin/env python
# -*- coding: utf-8 -*-

# DBpedia Reducer
# VI: Viktor Maruna

import sys
from collections import defaultdict
import json

reload(sys)
sys.setdefaultencoding('utf-8')

# data types in AVRO schema
AVRO = {'article':'string', 'type':'string', 'category':'array', 'template':'array', 'disambiguates':'array', 'property':'array'}

# preparing data types for AVRO
def AVRO_output(inp):

	out = defaultdict(list) # values dictionary

	for i in inp:
		out[str(i.keys()[0])].append(str(i.values()[0]))

	for i in out.keys(): #  format
		if AVRO[i] == 'string':
			out[i] = ''.join(out[i])	

	return json.dumps(dict(out), sort_keys=True) #  output JSON

###############################################################################
def main():

	data = [] # output data
	key = None # key
	prev_key = None # previous key

	try:
		for line in sys.stdin:  
			line = line.strip()
			key, value = line.split('\t', 1) # get key, value

			# first iteraton			
			if not prev_key:
				prev_key = key

			# if they're the same
			if prev_key == key:
				data.append(eval(value))
			else:
				data.append({'article':prev_key})
				print AVRO_output(data) # output
				prev_key = key # new key
				data = []
				data.append(eval(value)) # append

		if key == prev_key:
			# emit last key
			data.append({'article':prev_key}) # append
			print AVRO_output(data) # output

	except Exception, err:
		sys.stderr.write('Reducer ERROR (%s): %s\n' % (key, str(err)))

if __name__ == '__main__':
    main()

