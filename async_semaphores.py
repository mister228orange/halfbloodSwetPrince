import asyncio
import aiohttp
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup # pip install aiohttp aiodns
from time import time
from aiodecorators import Semaphore

""" 
Достаточно часто встречается прикладная проблема того, что даже с одной машины можно 
заддосить сервак, особенно если его писали всякие обоссанные вкатыши незнающие нотацию O
И свято уверенных что алгоритмы в жизни не нужны (не нужны вкатыши). Но задача поставлена, ограничить число паралельных 
запросов, ее нужно решить. Идеально подходит Декоратор семафор, ниже подробная реализация
"""




start = time()


async def antiddos(semaphore, session, page):
    async with semaphore:
        return await get_page(session, page)


from functools import wraps


def antiddos_decorator(max_concurrent=10): # its equal aiodecorators semaphore. run it to understand how it work, plain to see with that example
    def wrapp(func):
        semaphore = asyncio.Semaphore(max_concurrent)
        @wraps(func)
        async def wrapped(*args, **kwargs):
            async with semaphore:
                return await func(*args, **kwargs)

        return wrapped
    return wrapp

@antiddos_decorator(12)
async def get_page(
    session: aiohttp.ClientSession,
    page: int,
    **kwargs
) -> dict: # its requests just for example, u can replace it for any other
    global start
    url = f"https://acm.timus.ru/ranklist.aspx"
    delay = time() - start
    print(f'page №{page} start with delay {delay}')
    if page > 1:
        url = url + f'?from={(page - 1) * 25 + 1}'
    async with session.request('GET', url=url, **kwargs) as resp:


        data = await resp.text()
        soup = BeautifulSoup(data, "html.parser")
        table = soup.find('table', class_="ranklist")
        nav = table.findAll('tr', class_="navigation")
        for n in nav:
            n.decompose()
        flags = table.findAll('div', class_="flags-img")
        for country in flags:
            country.replaceWith(country.attrs.get('title'))
        res = pd.read_html(table.prettify(), index_col=0)[0]#, index_col=["Rank"])[0].drop(columns=[0])#
        delay = time() - start
        print(f'page №{page} end with delay {delay}')
        return res





async def main():
    semaphore = asyncio.Semaphore(10)
    async with aiohttp.ClientSession() as session:

        tasks = [get_page(session, page) for page in range(1, 6000)]

        d = await asyncio.gather(*tasks)
    res = pd.concat(d)
    res.to_csv("timus_ranked.csv")

if __name__ == '__main__':
    asyncio.run(main())
