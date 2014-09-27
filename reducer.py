#!/usr/bin/env python
# -*- coding: utf-8 -*-

# DBpedia Reducer
# VI: Viktor Maruna

import sys
import avro.schema
from avro.io import DatumReader, DatumWriter
from avro.datafile import DataFileReader, DataFileWriter
from collections import defaultdict

# data types in AVRO schema
AVRO = {"label":"string", "category":"array"}


# preparing data types for AVRO
def AVRO_output(inp):

	out = defaultdict(list)

	for i in inp:
		out[str(i.keys()[0])].append(str(i.values()[0]))

	for i in AVRO.keys():
		if AVRO[i] == "string":
			out[i] = ''.join(out[i])

	return dict(out)

###############################################################################
def main():

	schema = avro.schema.parse(open('dbpedia.avsc').read())
	#writer = DataFileWriter(sys.stdout, DatumWriter(), schema)
	writer = DataFileWriter(open("out.avro", "w"), DatumWriter(), schema)

	data = []
	key = None
	prev_key = None

	for line in sys.stdin:  
		try:
			line = line.strip()
			key, value = line.split('\t', 1)

			# first iteraton			
			if not prev_key:
				prev_key = key

			# if they're the same
			if prev_key == key:
				data.append(eval(value))
			else:
				writer.append(AVRO_output(data))
				prev_key = key
				data = []
				data.append(eval(value))

		except:
			pass

	if key == prev_key:
		# emit last key
		writer.append(AVRO_output(data))

	writer.close()

if __name__ == '__main__':
    main()

