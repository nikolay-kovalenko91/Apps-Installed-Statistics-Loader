# Apps Installed Loader
Reads TSV files with app installed data and loads that into DB(Redis). 

## Running App

```$ go run memc_load.go``

#### Running Options

1) ```-log```: a path to log file
2) ```-dry```: fake loading to DB
3) ```-pattern``` : TSV files pattern. ```./input_files/*.tsv.gz``` by default.
4) ```-idfa ,-gaid, -adid, -dvid```: DB servers hosts.`

### Running App in Docker

```INPUT_FILES_DIR=<a dir with input files> docker-compose up --build```

