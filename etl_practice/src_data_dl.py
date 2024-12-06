import requests, zipfile, io, os

r = requests.get('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/datasource.zip')
z = zipfile.ZipFile(io.BytesIO(r.content))
z.extractall(os.getcwd())

