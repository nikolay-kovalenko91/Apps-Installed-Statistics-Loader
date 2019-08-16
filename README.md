# Apps Installed Statistics Loader
What it does?
1) Parses tracker-service logs containing statistics about mobile phone apps installed by some users
2) Translates the log record into Protobuf message and loads it into Redis store

[A Logs file example here](https://cloud.mail.ru/public/LoDo/SfsPEzoGc)

## Running App

```$ go run loader.go <flags>```

#### Running Options(flags)

1) ```-log```: a file path to write the app logs in
2) ```-dry```: fake loading - does not save data into the store
3) ```-pattern``` : parsing logs files pattern. ```./input_files/*.tsv.gz``` by default.
4) ```-idfa ,-gaid, -adid, -dvid```: Redis servers hosts`

### Running App in Docker

```INPUT_FILES_DIR=<a dir with log files> docker-compose up```
