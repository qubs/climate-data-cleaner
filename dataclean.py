#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv, sys, math, numpy, json

# ===================================================================
# | Data Cleaner Utility											|
# | (c) Queen's University Biological Station 2015. 				|
# | Written by David Lougheed										|
# | Syntax: ./dataclean.py path/to/infile path/to/outfile 			|
# ===================================================================

def processData(fields, rawData):
	outFields = []

	for f in range(0, len(fields)):
		if fields[f] not in forbiddenFields:
			fieldData = []
			for row in rawData:
				fieldData.append(row[f])

			fieldDataClean = []

			for d in range(0, len(fieldData)):
				if fieldData[d] == "" or fieldData[d] == " " or fieldData[d] == None:
					fieldData[d] = numpy.nan
				else:
					fieldDataClean.append(fieldData[d])

			firstQuartile = numpy.percentile(numpy.array(fieldDataClean, numpy.float), 25).item()
			thirdQuartile = numpy.percentile(numpy.array(fieldDataClean, numpy.float), 75).item()
			iqr = thirdQuartile - firstQuartile

			lowerBound = firstQuartile - float(iqr) * aggression
			upperBound = thirdQuartile + float(iqr) * aggression

			removed = 0

			for d in range(0, len(fieldData)):
				if fieldData[d] != numpy.nan:
					if float(fieldData[d]) < lowerBound or float(fieldData[d]) > upperBound:
						fieldData[d] = ""
						removed += 1

			#print fields[f] + "\t\t25%: " + str(firstQuartile) + "\t75%: " + str(thirdQuartile) + "\tIQR: " + str(iqr) + "\tLower: " + str(round(lowerBound, 3)) + "\tUpper: " + str(round(upperBound, 3)) + "\tRemoved " + str(removed)

			outFields.append(fieldData)
		else:
			fieldData = []
			for row in rawData:
				fieldData.append(row[f])

			outFields.append(fieldData)

	return outFields


forbiddenFields = ["Date/Time", "Battery"] # what fields get ignored?
aggression = 1.5 # range: (0, ∞) lower value means more aggressive removal
method = 2 # method for determining outliers
chunkSize = 750

fieldData = {}
limits = {
	"Water T1": [-70, 70],
	"Water T2": [-70, 70],
	"WTemp1": [-70, 70],
	"WTemp2": [-70, 70],

	"Temp": [-80, 80],
	"Air Temp": [-80, 80],

	"Soil T1": [-70, 100],
	"Soil T2": [-70, 100],
	"15cm Soil Tg": [-70, 100],
	"5cm Soil Tg": [-70, 100]
}
enableOutliersSearch = {
	"WSpd": False,
	"Gust": False,
}

filename = sys.argv[1]
outfile = sys.argv[2]
config = "./config.json"

if len(sys.argv) < 3:
	print "Syntax: ./dataclean.py path/to/infile path/to/outfile"
	exit()

with open(config, "rU") as configfile:
	configData  = json.load(configfile)
	
	method = configData["method"]
	aggression = configData["aggression"]
	chunkSize = configData["chunkSize"]
	fieldData = configData["fields"]

	#	There should be about 25 chunks to a year's worth of seasonal data.
	#	For non-seasonal data, this number shouldn't matter too much.

	with open(filename, "rU") as csvData:
		dataReader = csv.reader(csvData, dialect="excel")

		rawData = []
		for row in dataReader:
			rawData.append(row)

		stationName = rawData[0][1]

		fields = rawData[1]
		outFields = []

		firstRow = rawData[0]

		rawData.remove(rawData[0]) # remove station name
		rawData.remove(rawData[0]) # remove headers

		rawDataOffsets = []
		outFieldsOffsets = []

		for r in range(0, len(rawData)):
			for f in range(0, len(rawData[r])):
				if rawData[r][f] != "" and not fieldData[fields[f]]["forbidden"]:
					if float(rawData[r][f]) < fieldData[fields[f]]["bounds"][0] or float(rawData[r][f]) > fieldData[fields[f]]["bounds"][1]:
						rawData[r][f] = ""

		if method == 3:
			print "Generating frames..."

			for r in range(0, chunkSize):
				sys.stdout.write("\r" + str(int(round(float(r + 1) / float(chunkSize) * 100))) + "%");
				sys.stdout.flush();
				rawDataOffsets.append(rawData[r:] + rawData[:r])

			print ""

		if method == 1:
			outFields = processData(fields, rawData)
		elif method == 2:
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
			for rd in range(0, len(rawDataOffsets)):
				chunks = []
				numChunks = int(math.ceil(float(len(rawDataOffsets[rd])) / float(chunkSize)))

				for c in range(0, numChunks):
					sys.stdout.write("\rOffset " + str(rd + 1) + " of " + str(len(rawDataOffsets)) + " | Processed chunk " + str(c + 1) + " of " + str(numChunks) + " ");
					sys.stdout.flush();
					chunks.append(processData(fields, rawDataOffsets[rd][c * chunkSize:(c + 1) * chunkSize]))

				outFieldsOffset = chunks[0]

				for c in range(1, numChunks):
					for i in range(0, len(chunks[c])):
						for i2 in chunks[c][i]:
							outFieldsOffset[i].append(i2)

				outFieldsOffsets.append(outFieldsOffset)

			# rotate items back into place

			for o in range(0, len(outFieldsOffsets)):
				for i in range(0, len(outFieldsOffsets[o])):
					outFieldsOffsets[o][i] = outFieldsOffsets[o][i][-o:] + outFieldsOffsets[o][i][:-o]


			for z in range(0, len(fields)):
				outFields.append(["None"] * len(outFieldsOffsets[0][0]))

			for f in range(0, len(fields)):
				numOffsets = len(outFieldsOffsets)
				minimumPresent = int(round(float(len(outFieldsOffsets)) / 2.0))

				for v in range(0, len(outFieldsOffsets[0][0])):

					numPresent = 0
					presentOffset = -1

					for o in range(0, numOffsets):
						if outFieldsOffsets[o][f][v] != "" and outFieldsOffsets[o][f][v] != " ":
							numPresent += 1
							presentOffset = o

					if numPresent >= minimumPresent:
						outFields[f][v] = outFieldsOffsets[presentOffset][f][v]
					else:
						outFields[f][v] = ""

			print ""

		with open(outfile, 'wb') as outfile:
			dataWriter = csv.writer(outfile, dialect="excel")
			dataWriter.writerow(firstRow)
			dataWriter.writerow(fields)

			for i in range(0, len(outFields[0])):
				row = []
				for of in outFields:
					value = of[i]
					if value == numpy.nan or str(value) == "nan" or value == str(numpy.nan):
						value = ""

					row.append(value)

				dataWriter.writerow(row)
