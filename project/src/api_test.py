import requests

#Url to upload the files via POST Api
url = "http://127.0.0.1:5000/upload_data"

# Upload departments.csv
files = {'file': ('departments.csv', open('../files/departments.csv', 'rb'))}
response = requests.post(url, files=files)
print(response.json())

# Upload jobs.csv
files = {'file': ('jobs.csv', open('../files/jobs.csv', 'rb'))}
response = requests.post(url, files=files)
print(response.json())

# Upload jobs.csv
files = {'file': ('hired_employees.csv', open('../files/hired_employees.csv', 'rb'))}
response = requests.post(url, files=files)
print(response.json())