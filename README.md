The file structure consists of 4 folders
- instance: stores de db file (Sqlite)
- myVenv: stores the virtual environment in which everything has been set up for this implementation
- project
  - files: stores the files to be uploaded to the DB using the REST API
  - src:
    - api_test.py tests the files upload to the db
    - api_implementation.py stores the logic of the uploads API and the 2 SQL endpoints
    - Queries_endpoints.txt stores the SQL statements as an info file
- uploads: stores the csv files once received from the API call
