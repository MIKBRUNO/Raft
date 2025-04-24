from typing import Any, Annotated

from fastapi import FastAPI, Depends

from depends import get_dictionary, lifespan

from dictionary import Dictionary


app = FastAPI(lifespan=lifespan)


@app.post("/set")
async def update(key: str, value: Any, d: Annotated[Dictionary, Depends(get_dictionary)]) -> Any:
    return await d.update(key, value)


@app.get("/get/{key}")
async def get(key: str, d: Annotated[Dictionary, Depends(get_dictionary)]) -> Any:
    return d.get(key)


@app.delete("/erase/{key}")
async def delete(key: str, d: Annotated[Dictionary, Depends(get_dictionary)]) -> Any:
    return await d.delete(key)
