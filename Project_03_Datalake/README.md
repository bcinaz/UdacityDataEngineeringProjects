# Data Lake Project with Apache Spark

In this project, we have built an ETL pipeline which extracts the song and log data from Udacity S3 bucket, processes it using Spark and loads the processed data back into an S3 bucket as a set of dimensional tables in Spark parquet files.

## Usage

1. Set AWS config variables written in `dl.cfg` 

```
[AWS]
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
```
2. Create an S3 bucket (if you haven't created one before) and replace the `output_data` variable in the `main()` function with your own S3 bucket name. For example in our case it looks like this:  `output_data = "s3a://aws-emr-resources-554247792206-us-west-2"`
3. Then you can run the ETL pipeline as follows:

```sh
$ python etl.py 
````

or if you can open a new notebook and write the following code to run the etl file
```python
%run etl.py
```

## Datasets

The following two main datasets are used:

- `s3a://udacity-dend/song_data/*/*/*` - JSON files containing meta information about song/artists data

- `s3a://udacity-dend/log_data/*/*` - JSON files containing log events from the Sparkify app