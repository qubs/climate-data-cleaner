# Data Clean
**© Queen's University Biological Station 2015.**

A utility developed to clean up data produced by climate stations.

## Syntax

    ./dataclean.py path/to/infile path/to/outfile

## Configuration

All configuration is stored in the `config.json` file stored in the same directory as the script. The following fields are available for configuration:

### method

Values: `1` through `3`

Determines which method is used to determine outliers in the data. A higher number typically means a slower, more accurate method.

#### Method 1

In this method, the entire dataset is analysed at once. Percentiles are calculated for the entire file, and outliers are outside the range of `[25% percentile - IQR × aggression, 75% percentile + IQR × aggression]. This method is fastest, but can result in unnecessary data removal, especially in wide-ranging seasonal values like temperature.

#### Method 2

Method 2 divides up the data into chunks, specified by the `chunkSize` parameter. Each chunk is treated like the entire dataset was in the first method. This is particularely useful for seasonal data, where each chunk can be analysed for errors independently. For example, method 1 may include a temperature of 0°C as acceptable in a summer section of temperature data, even if it is an equipment error, because 0°C is an reasonable value at other times in the year. However, when the data is chunked, this data would be caught as an outlier and removed. See the `chunkSize` configuration information for advice on setting this configuration parameter.

#### Method 3

Lorem ipsum dolor sit amet, consectetur adipisicing elit. Sit libero vero, doloremque, labore odit a architecto aperiam suscipit! Ad facere nesciunt aliquid distinctio ipsum ex accusantium earum soluta quasi eveniet!


### aggression

Lorem ipsum dolor sit amet, consectetur adipisicing elit. Similique consectetur explicabo est accusamus nam ipsam assumenda eligendi maiores quasi, incidunt hic alias repellat dolore dicta mollitia itaque a ea esse.

### chunkSize

Lorem ipsum dolor sit amet, consectetur adipisicing elit. Pariatur libero, labore vel unde quos cumque, aperiam distinctio quod, repellendus hic obcaecati error modi facere dolor repellat similique. Amet, molestias, cum.

### fields

Lorem ipsum dolor sit amet, consectetur adipisicing elit. Repellendus consequuntur quisquam quae fugiat atque animi eum blanditiis facere iusto itaque ex debitis quo, quibusdam, pariatur nobis harum ut, rem dolorem.

#### forbidden

**Format:** `true` or `false`

If this field does **not** need to be analysed for outliers or changed at all, use a value of `true`. Otherwise, use `false`.

#### bounds

**Format:** `[###, ###]` where the first number is the lower boundary and the second number is the upper boundary.

This specifies the absolute minimum and maximum values that are acceptable. Values that are not within this range are removed without any sort of analysis or inclusion in quartile calculations. These should be set to well beyond reasonable values that may be included in the data that can be removed without analysis. For example, lake water temperatures of 100°C or above are entirely unreasonable and do not need to be tested against percentiles to be removed.

#### outlierSearch

**Format:** `true` or `false`

If this field does not or should not be analysed using a percentile- and iqr-based search, set to `false`. Otherwise, set to `true`. It may be useful to disable this option if the measurement is one prone to sudden and extreme changes that may last for shots amount of time; for example wind speeds or directions.