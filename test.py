import os
v=input('Varible de entorno: ')
#api_key = os.environ["API_KEY"]
api_key = os.environ.get(v) # mejor
print(api_key)