from fastapi import FastAPI
app = FastAPI()

@app.get("/")
def home():
    return {"Hello" : "FastAPI"}


@app.get("/sum")
def suma(a:int, b:int):
    return a+b

@app.get("/items/")
async def read_item(user: str):
    return f"hola {user}"
