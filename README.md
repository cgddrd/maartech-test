# Maartech Technical Test Submission

**Author: Connor Goddard**

## Description
This Python application allows users to import the contents of CSV data files located in a target folder to dedicated tables in a PostgreSQL database. 

## Usage

To get started, type the following in the terminal/Command Prompt:

```
pip install -r requirements.txt

python ./run.py --help
```

## Configuration

You can specify database connection settings and the target folder path via a YAML configuration file (default: `./config.yml). 

The structure of this configuration file should be as follows:

```
db:
    host: <HOST_NAME>
    port: <PORT_NUMBER>
    database: <DATABASE_NAME>
    user: <USER_NAME>
    password: <PASSWORD>

target_path: <FILE_PATH_TO_FOLDER_CONTAINING_CSV_FILES>
```


