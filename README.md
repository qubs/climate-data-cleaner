# Data Cleaner
**© Queen's University Biological Station 2015-2016.**

A utility developed to clean up data produced by climate stations.

## Syntax

    ./dataclean.py path/to/infile path/to/outfile

## Configuration

All configuration is stored in the `config.json` file stored in the same directory as the script. The following fields
are available for configuration:

### `method`

Values: `1` through `3`

Determines which method is used to determine outliers in the data. A higher number typically means a slower, more
accurate method.

#### Method 1

In this method, the entire dataset is analysed at once. Percentiles are calculated for the entire file, and outliers are
outside the range of `[25% percentile - IQR × aggression, 75% percentile + IQR × aggression]`. This method is fastest,
but can result in unnecessary data removal, especially in wide-ranging seasonal values like temperature.

#### Method 2

Method 2 divides up the data into chunks, specified by the `chunkSize` parameter. Each chunk is treated like the entire
dataset was in the first method. This is particularely useful for seasonal data, where each chunk can be analysed for
errors independently. For example, method 1 may include a temperature of 0°C as acceptable in a summer section of
temperature data, even if it is an equipment error, because 0°C is an reasonable value at other times in the year.
However, when the data is chunked, this data would be caught as an outlier and removed. See the `chunkSize`
configuration information for advice on setting this configuration parameter.

#### Method 3

Method 3 performs multiple chunk-based analyses on the data, rotating the array the full length of a chunk, to reduce
false removals. It is significantly slower than methods 1 or 2, but results in less accidental false data/false
removals.

### `quartileDistance`

**Values:** `(0, ∞)` (Suggested: `1.5`)

The quartile distance parameter is used in statistical analysis in the following formula, used in methods 1 and 2:

    [25% percentile - IQR × quartileDistance, 75% percentile + IQR × quartileDistance]

A higher number results in more *lenient* bounds, whereas a value approaching 0 results in more aggressive pruning of
data and a smaller acceptable range.

### `stdevDistance`

**Values:** `(0, ∞)` (Suggested: `2`)

The standard deviation distance parameter is used to prune data more than `stdevDistance × stdev` above or below the
mean, where `stdev` is the standard deviation. A higher number results in more *lenient* bounds, whereas a value
approaching 0 results in more aggressive pruning of data and a smaller acceptable range.

### `chunkSize`

**Values:** `[5, *length of data*]`

The size of each chunk, and if method 3 is used the number of offset frames generated as well. What this value is set to
depends on the type of data used, and if the data is temporal and seasonal, the range of time the data covers as well as
the time interval between points.

### `fields`

This contains a list of any field that would be in your dataset and the parameters for each individual dataset.

#### `forbidden`

**Format:** `true` or `false`

If this field does **not** need to be analysed for outliers or changed at all, use a value of `true`. Otherwise, use
`false`.

#### `bounds`

**Format:** `[###, ###]` where the first number is the lower boundary and the second number is the upper boundary.

This specifies the absolute minimum and maximum values that are acceptable. Values that are not within this range are
removed without any sort of analysis or inclusion in quartile calculations. These should be set to well beyond
reasonable values that may be included in the data that can be removed without analysis. For example, lake water
temperatures of 100°C or above are entirely unreasonable and do not need to be tested against percentiles to be removed.

#### `outlierSearch`

**Format:** `true` or `false`

If this field does not or should not be analysed using a percentile- and iqr-based search, set to `false`. Otherwise,
set to `true`. It may be useful to disable this option if the measurement is one prone to sudden and extreme changes
that may last for short amount of time; for example wind speeds or directions.
