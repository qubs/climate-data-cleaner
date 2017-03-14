#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv, sys, math, numpy, json, multiprocessing, itertools, time, resource, gc, objgraph

# ┌───────────────────────────────────────────────────────────┐
# │ Data Cleaner Utility                                      │
# │ (c) Queen's University Biological Station 2015-2016.      │
# │ Written by David Lougheed.                                │
# │ Syntax: ./data-cleaner.py path/to/infile path/to/outfile  │
# └───────────────────────────────────────────────────────────┘

MIN_CHUNK_SIZE = 5
CONFIG_FILE = "./config.json"

def processData(fields, rawData, settings):
	"""Processes data using outlier and standard deviation tests"""

	outFields = []

	for f in range(0, len(fields)):
		fieldData = []

		if not settings["fields"][fields[f]]["forbidden"]:
			# Move all raw data from the current particular field (i.e. column) into the field data list.

			for row in rawData:
				fieldData.append(row[f])

			# Clean up the data. Set it to a numpy NaN if it's blank. Otherwise, append as-is. The clean data is used
			# for calculating statistical information, since numpy's quartile values treat NaN, ironically,
			# like a number.

			fieldDataClean = []

			for d in range(0, len(fieldData)):
				if fieldData[d] == "" or fieldData[d] == " " or fieldData[d] == None:
					fieldData[d] = numpy.nan
				else:
					fieldDataClean.append(fieldData[d])

			# Calculate the quartile values and use them to calculate the boundaries of outliers in the column.

			if len(fieldDataClean) > 0:
				#start = time.clock()

				nPFloatFieldDataClean = numpy.array(fieldDataClean, numpy.float)

				firstQuartile = numpy.percentile(nPFloatFieldDataClean, 25).item()
				thirdQuartile = numpy.percentile(nPFloatFieldDataClean, 75).item()

				fIQRA = float(thirdQuartile - firstQuartile) * settings["quartileDistance"] # iqr * factor

				mn = numpy.mean(nPFloatFieldDataClean).item()
				stdStretch = settings["stdevDistance"] * numpy.std(nPFloatFieldDataClean).item()

				#end = time.clock()
				#print("A: {}".format(end - start))

				#start = time.clock()

				for d in fieldData:
					if d != numpy.nan:
						fieldDataPointCache = float(d)
						if ((fieldDataPointCache < firstQuartile - fIQRA or fieldDataPointCache > thirdQuartile + fIQRA)
								and (fieldDataPointCache < mn - stdStretch or fieldDataPointCache > mn + stdStretch)):
							d = numpy.nan

				#end = time.clock()
				#print("B: {}".format(end - start))

				del nPFloatFieldDataClean
			else:
				for d in fieldData:
					d = numpy.nan

			outFields.append(fieldData)
			del fieldDataClean
		else:
			# If the field is forbidden, just push all the data into the output as-is.

			for row in rawData:
				fieldData.append(row[f])

			outFields.append(fieldData)
		del fieldData

	return outFields

def processOffset(offsetIndex, rawDataOffsets, numChunks, numFields, fields, settings):
	chunks = []
	outFieldsOffset = []
	offset = rawDataOffsets[offsetIndex]

	for c in range(0, numChunks):
		chunks.append(processData(fields, offset[c * settings["chunkSize"]:(c + 1) * settings["chunkSize"]], settings))

		if c == 0:
			outFieldsOffset = chunks[0]
		else:
			for i in range(0, numFields):
				outFieldsOffset[i].extend(chunks[c][i])
				# for i2 in chunks[c][i]:
				# 	outFieldsOffset[i].append(i2)

	# sys.stdout.write("\rProcessed offset {} of {} ".format(offsetIndex, len(rawDataOffsets)));
	# sys.stdout.flush();

	return outFieldsOffset

def main():
	if len(sys.argv) < 3:
		print("Syntax: ./dataclean.py path/to/infile path/to/outfile")
		exit()

	filename = sys.argv[1]
	outfile = sys.argv[2]

	# Default values:

	# quartileDistance: range: (0, ∞) lower value means more aggressive removal via # of IQRs away from 25/75% quartile
	# stdevDistance: range: (0, ∞) lower value means more aggressive removal via # of standard deviations away from mean

	settings = {
		"quartileDistance": 1.5,
		"stdevDistance": 2,
		"threshold": 0.40,
		"method": 3,
		"chunkSize": 850,
		"skipRows": 2,
		"headerRow": 2,
		"fields": {}
	}

	dataLength = -1
	skipRows = 2
	headerRow = 2

	skippedRows = []

	with open(CONFIG_FILE, "rU") as configFile:
		configData = json.load(configFile)

		# Import configuration from file.

		for k in settings.keys():
			if k in configData.keys():
				settings[k] = configData[k]

		# There should be about 25 chunks to a year's worth of seasonal data. For non-seasonal data, this number
		# shouldn't matter too much.

		if settings["chunkSize"] < MIN_CHUNK_SIZE:
			print("Error: Chunk size minimum is {}, chunk size is currently set to {}.".format(MIN_CHUNK_SIZE,
			                                                                                   settings["chunkSize"]))
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

			# Out field structure:
			# [
			#	[field1val1, field1val2, field1val3, ...],
			#	[field2val1, field2val2, field2val3, ...],
			#	[field3val1, field3val2, field3val3, ...],
			#	...
			# ]

			outFields = []

			for f in fields:
				if f not in settings["fields"]:
					print("Error: undefined field parameters for " + f + ".")
					exit()

			firstRow = rawData[0]

			for r in range(0, settings["skipRows"]):
				# Remove rows that do not contain data.
				skippedRows.append(rawData[0])
				rawData.remove(rawData[0])

			dataLength = len(rawData)

			if dataLength < settings["chunkSize"]:
				print("Error: Chunk size is larger than dataset.")
				exit()

			for r in range(0, dataLength):
				for f in range(0, len(rawData[r])):
					if rawData[r][f] != "" and not settings["fields"][fields[f]]["forbidden"]:
						if (float(rawData[r][f]) < settings["fields"][fields[f]]["bounds"][0]
								or float(rawData[r][f]) > settings["fields"][fields[f]]["bounds"][1]):
							rawData[r][f] = ""

			if settings["method"] == 1:
				# Method 1
				# Perform a statistical analysis on the entire dataset at once, without dividing it up.
				# Can result in unnecessary data removal.

				outFields = processData(fields, rawData, settings)
			elif settings["method"] == 2:
				# Method 2
				# Chunk up the dataset into sizes specified by chunkSize, then analyse each one individually and
				# eliminate outliers in the chunk based on those results.
				# Takes more time, but is more accurate than method 1.

				chunks = []
				numChunks = int(math.ceil(float(len(rawData)) / float(chunkSize)))

				for c in range(0, numChunks):
					sys.stdout.write("\rProcessed chunk " + str(c + 1) + " of " + str(numChunks));
					sys.stdout.flush();
					chunks.append(processData(fields, rawData[c * chunkSize:(c + 1) * chunkSize], settings))

				print("Processed {} chunks.".format(numChunks))

				outFields = chunks[0]

				for c in range(1, numChunks):
					for i in range(0, len(chunks[c])):
						for i2 in chunks[c][i]:
							outFields[i].append(i2)

				print()
			elif settings["method"] == 3:
				# Method 3
				# Perform method 2 on multiple offsets/rotations of the data to reduce false positives and negatives.
				# Takes a long time.

				# When the multiframe method is used, an array of multiple raw datasets is created, each offset by 1
				# more than the last, resulting in an array of a full 1-chunk rotation period of data. The benefits of
				# this are that false positives or negatives can be found based on entire chunks which may be outliers.

				sys.stdout.write("Generating frames... ")
				sys.stdout.flush();

				rawDataOffsets = [rawData[r:] + rawData[:r] for r in range(0, settings["chunkSize"])]

				sys.stdout.write("Done.")
				sys.stdout.flush();

				print()

				numChunks = int(math.ceil(float(len(rawDataOffsets[0])) / float(settings["chunkSize"])))
				numOffsets = len(rawDataOffsets)

				print()
				print("Using {} chunks.".format(numChunks))

				outFieldsOffsets = [rd for rd in range(0, numOffsets)]

				# p = multiprocessing.Pool(processes = 4, maxtasksperchild = 2) # Combat memory hogging with max tasks
				# outFieldsOffsets = p.starmap(processOffset, zip(outFieldsOffsets, itertools.repeat(rawDataOffsets),
				#                              itertools.repeat(numChunks), itertools.repeat(numFields),
				#                              itertools.repeat(fields), itertools.repeat(settings)), chunksize=50)

				for rd in range(0, len(rawDataOffsets)):
					start = time.clock()
					outFieldsOffsets[rd] = processOffset(rd, rawDataOffsets, numChunks, numFields, fields, settings)
					end = time.clock()
					# objgraph.show_most_common_types()

					sys.stdout.write("\rOffset {} of {} in {} with memory {}".format(rd + 1, numOffsets, end - start,
						resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
					sys.stdout.flush();
					# outFieldsOffsets.append(processOffset(numChunks, numFields, fields, settings, rawDataOffsets[rd]))

				print("Processed {} offsets.".format(numChunks))

				# Rotate items back into place.

				for o in range(0, len(outFieldsOffsets)):
					sys.stdout.write("\rRotated chunk " + str(o + 1) + " of " + str(numOffsets) + " ");
					sys.stdout.flush()

					for i in range(0, len(outFieldsOffsets[o])):
						outFieldsOffsets[o][i] = outFieldsOffsets[o][i][-o:] + outFieldsOffsets[o][i][:-o]

				print()

				minimumPresent = int(round(float(numOffsets) * settings["threshold"]))

				for f in range(0, numFields):
					sys.stdout.write("\rTesting field {} of {} ".format(f + 1, numFields));
					sys.stdout.flush()

					# Generate a properly proportioned array for fitting the results into after they have been
					# analysed to determine whether the value should be kept.

					outFields.append([None] * dataLength)

					for v in range(0, dataLength):
						numPresent = 0
						presentOffset = -1

						for o in range(0, numOffsets):
							if (outFieldsOffsets[o][f][v] != "" and str(outFieldsOffsets[o][f][v]) != "nan"
									and outFieldsOffsets[o][f][v] != numpy.nan):
								numPresent += 1
								presentOffset = o

						if numPresent >= minimumPresent:
							outFields[f][v] = outFieldsOffsets[presentOffset][f][v]
						else:
							outFields[f][v] = ""

				print()
			else:
				print("Error: Invalid method.")
				exit()

			with open(outfile, "w") as outfile:
				dataWriter = csv.writer(outfile, dialect="excel")

				for sr in skippedRows:
					print(sr)
					dataWriter.writerow(sr)

				for i in range(0, dataLength):
					row = []
					for of in outFields:
						value = of[i]
						if value == numpy.nan or str(value) == "nan" or value == str(numpy.nan):
							value = ""

						row.append(value)

					dataWriter.writerow(row)

					sys.stdout.write("\rWrote row {} of {} ".format(i + 1, dataLength));
					sys.stdout.flush()

				print()
				sys.stdout.flush()

if __name__ == "__main__":
	main()
