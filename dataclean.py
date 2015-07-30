#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv, sys, math, numpy, json

# ===================================================================
# | Data Cleaner Utility											|
# | (c) Queen's University Biological Station 2015. 				|
# | Written by David Lougheed										|
# | Syntax: ./dataclean.py path/to/infile path/to/outfile 			|
# ===================================================================

aggression = 1.5 # range: (0, ∞) lower value means more aggressive removal
method = 3 # method for determining outliers
chunkSize = 750
minChunkSize = 5
skipRows = 2
headerRow = 2

skippedRows = []

fieldSettings = {}

filename = sys.argv[1]
outfile = sys.argv[2]
config = "./config.json"

def processData(fields, rawData):
	outFields = []

	for f in range(0, len(fields)):
		if not fieldSettings[fields[f]]["forbidden"]:
			fieldData = []

			# Move all raw data from the current particular field (i.e. column) into the field data list.

			for row in rawData:
				fieldData.append(row[f])

			# Clean up the data. Set it to a numpy NaN if it's blank. Otherwise, append as-is.
			# The clean data is used for calculating statistical information, since numpy's
			# quartile values treat NaN, ironically, like a number.

			fieldDataClean = []

			for d in range(0, len(fieldData)):
				if fieldData[d] == "" or fieldData[d] == " " or fieldData[d] == None:
					fieldData[d] = numpy.nan
				else:
					fieldDataClean.append(fieldData[d])

			# Calculate the quartile values and use them to calculate the boundaries of outliers in the column.

			nPFloatFieldDataClean = numpy.array(fieldDataClean, numpy.float)

			firstQuartile = numpy.percentile(nPFloatFieldDataClean, 25).item()
			thirdQuartile = numpy.percentile(nPFloatFieldDataClean, 75).item()
			iqr = thirdQuartile - firstQuartile

			fIQRA = float(iqr) * aggression

			lowerBound = firstQuartile - fIQRA
			upperBound = thirdQuartile + fIQRA

			for d in range(0, len(fieldData)):
				if fieldData[d] != numpy.nan:
					if float(fieldData[d]) < lowerBound or float(fieldData[d]) > upperBound:
						fieldData[d] = ""

			#print fields[f] + "\t\t25%: " + str(firstQuartile) + "\t75%: " + str(thirdQuartile) + "\tIQR: " + str(iqr) + "\tLower: " + str(round(lowerBound, 3)) + "\tUpper: " + str(round(upperBound, 3)) + "\tRemoved " + str(removed)

			outFields.append(fieldData)
		else:
			# If the field is forbidden, just push all the data into the output as-is.

			fieldData = []
			for row in rawData:
				fieldData.append(row[f])

			outFields.append(fieldData)

	return outFields

if len(sys.argv) < 3:
	print "Syntax: ./dataclean.py path/to/infile path/to/outfile"
	exit()

with open(config, "rU") as configfile:
	configData  = json.load(configfile)
	
	method = configData["method"]
	aggression = configData["aggression"]
	chunkSize = configData["chunkSize"]
	fieldSettings = configData["fields"]

	#	There should be about 25 chunks to a year's worth of seasonal data.
	#	For non-seasonal data, this number shouldn't matter too much.

	if chunkSize < minChunkSize:
		print "Error: Chunk size minimum is " + str(minChunkSize) + ", set to " + str(chunkSize) + "."
		exit()

	with open(filename, "rU") as csvData:
		dataReader = csv.reader(csvData, dialect="excel")

		# The raw data format is as follows:
		# [
		#	[field1val1, field2val1, field3val1],
		#	[field1val2, field2val2, field3val2],
		#	[field1val3, field2val3, field3val3],
		#	...
		# ]

		rawData = []
		for row in dataReader:
			rawData.append(row)

		stationName = rawData[0][1]

		fields = rawData[headerRow - 1]
		numFields = len(fields)
		outFields = []

		firstRow = rawData[0]

		for r in range(0, skipRows):
			# Remove rows that do not contain data.
			skippedRows.append(rawData[0])
			rawData.remove(rawData[0])

		if len(rawData) < chunkSize:
			print "Error: Chunk size is larger than dataset."
			exit()

		rawDataOffsets = []
		outFieldsOffsets = []

		for r in range(0, len(rawData)):
			for f in range(0, len(rawData[r])):
				if rawData[r][f] != "" and not fieldSettings[fields[f]]["forbidden"]:
					if float(rawData[r][f]) < fieldSettings[fields[f]]["bounds"][0] or float(rawData[r][f]) > fieldSettings[fields[f]]["bounds"][1]:
						rawData[r][f] = ""

		if method == 1:
			# Method 1
			# Perform a statistical analysis on the entire dataset at once, without dividing it up.
			# Can result in unnecessary data removal.

			outFields = processData(fields, rawData)
		elif method == 2:
			# Method 2
			# Chunk up the dataset into sizes specified by chunkSize, then analyse each one
			# individually and eliminate outliers in the chunk based on those results.
			# Takes more time, but is more accurate than method 1.

			chunks = []
			numChunks = int(math.ceil(float(len(rawData)) / float(chunkSize)))

			for c in range(0, numChunks):
				sys.stdout.write("\rProcessed chunk " + str(c + 1) + " of " + str(numChunks));
				sys.stdout.flush();
				chunks.append(processData(fields, rawData[c * chunkSize:(c + 1) * chunkSize]))

			outFields = chunks[0]

			for c in range(1, numChunks):
				for i in range(0, len(chunks[c])):
					for i2 in chunks[c][i]:
						outFields[i].append(i2)

			print ""
		elif method == 3:
			# Method 3
			# Perform method 2 on multiple offsets/rotations of the data to reduce false positives
			# and negatives. Takes a long time.

			# When the multiframe method is used, an array of multiple raw datasets is created,
			# each offset by 1 more than the last, resulting in an array of a full 1-chunk
			# rotation period of data. The benefits of this are that false positives or
			# negatives can be found based on entire chunks which may be outliers.

			for r in range(0, chunkSize):
				sys.stdout.write("\rGenerated frame " + str(r + 1) + " of " + str(chunkSize) + " ");
				sys.stdout.flush();

				rawDataOffsets.append(rawData[r:] + rawData[:r])

			print ""

			numChunks = int(math.ceil(float(len(rawDataOffsets[0])) / float(chunkSize)))
			numOffsets = len(rawDataOffsets)

			print str(numChunks) + " chunks created"

			for rd in range(0, len(rawDataOffsets)):
				sys.stdout.write("\rOffset " + str(rd + 1) + " of " + str(numOffsets) + " ");
				sys.stdout.flush();

				chunks = []

				for c in range(0, numChunks):
					chunks.append(processData(fields, rawDataOffsets[rd][c * chunkSize:(c + 1) * chunkSize]))

				outFieldsOffset = chunks[0]

				for c in range(1, numChunks):
					for i in range(0, len(chunks[c])):
						for i2 in chunks[c][i]:
							outFieldsOffset[i].append(i2)

				outFieldsOffsets.append(outFieldsOffset)

			# Rotate items back into place.

			print ""

			for o in range(0, len(outFieldsOffsets)):
				sys.stdout.write("\rRotated chunk " + str(o + 1) + " of " + str(numOffsets) + " ");
				sys.stdout.flush()

				for i in range(0, len(outFieldsOffsets[o])):
					outFieldsOffsets[o][i] = outFieldsOffsets[o][i][-o:] + outFieldsOffsets[o][i][:-o]
				
			print ""

			minimumPresent = int(round(float(numOffsets) / 2.0)) + 100

			for f in range(0, numFields):
				sys.stdout.write("\rTesting field " + str(f + 1) + " of " + str(numFields) + " ");
				sys.stdout.flush()

				# Generate a properly proportioned array for fitting the results into after they have been
				# analysed to determine whether the value should be kept.

				outFields.append([None] * len(outFieldsOffsets[0][0]))

				for v in range(0, len(outFieldsOffsets[0][0])):

					numPresent = 0
					presentOffset = -1

					for o in range(0, numOffsets):
						if outFieldsOffsets[o][f][v] != "" and str(outFieldsOffsets[o][f][v]) != "nan" and outFieldsOffsets[o][f][v] != numpy.nan:
							numPresent += 1
							presentOffset = o

					if numPresent >= minimumPresent:
						outFields[f][v] = outFieldsOffsets[presentOffset][f][v]
					else:
						outFields[f][v] = ""

			print ""

		with open(outfile, 'wb') as outfile:
			dataWriter = csv.writer(outfile, dialect="excel")

			for sr in skippedRows:
				dataWriter.writerow(sr)

			numRows = len(outFields[0])

			for i in range(0, numRows):
				row = []
				for of in outFields:
					value = of[i]
					if value == numpy.nan or str(value) == "nan" or value == str(numpy.nan):
						value = ""

					row.append(value)

				dataWriter.writerow(row)

				sys.stdout.write("\rWrote row " + str(i + 1) + " of " + str(numRows) + " ");
				sys.stdout.flush()

			print ""
			sys.stdout.flush()
