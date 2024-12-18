import os
import json
import time
import asyncio
import hashlib
import logging
import aiofiles
import subprocess
import pandas as pd
from pathlib import Path
from functools import reduce
from datetime import datetime
from bs4 import BeautifulSoup
# from curl_cffi.requests import Session
from curl_cffi.requests import AsyncSession


logger  =  logging.getLogger("BOOKSY")

BASE_DIR =  Path(__file__).resolve().parent

HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0',
  'Accept': '*/*',
  'Accept-Language': 'en-US,en;q=0.5',
  'Accept-Encoding': 'gzip, deflate',
  'Referer': 'https://booksy.com/',
  'X-Api-Key': 'web-e3d812bf-d7a2-445d-ab38-55589ae6a121',
}


async def append_link(url : str,):
    async with aiofiles.open(BASE_DIR / "bad.txt","a") as links:
        await links.write(f"{url}\n")


class APISAVER:
    def __init__(self) -> None:
        self.client =  AsyncSession()
        
        self.api_point = "http://127.0.0.1:8000/api/booksy/"
        self.token = "token f4e38ad1bcb9e4b5781be56ff87093f52af1d5f9"

        self.headers =  self.init_headers()
                
    def __str__(self) -> str:
        return f"{self.panel_domain}"
    
    
    def init_headers(self):
        return {
                "Authorization": self.token,
                "Accept": "application/json",
                'Content-Type': 'application/json',
                
                }

    def dump_data(self, data: dict) -> str:
        return json.dumps(data)
        
    async def create(self, data : dict, timeout: int = 301):
        response  =  await self.client.post(
                self.api_point,headers=self.headers,
                data=self.dump_data(data),timeout=timeout
        )
        
        json_content = response.headers.get('content-type') == 'application/json'        
        status_code  =  response.status_code    
        
        if json_content and status_code in [200,201]:
            return  response.json()  
        
        else:
            if "Ensure this field" in str( response.text):
                await append_link(data['url'])
            else:
                print( response.json() if json_content else  response.text)
        
    
    
    
    async def update(self,obj_id : int ,data : dict, timeout: int = 301):
        
        response  =  await self.client.patch(self.api_point + str(obj_id) + "/",
                headers=self.headers,data=self.dump_data(data),timeout=timeout
        )
        
        json_content = response.headers.get('content-type') == 'application/json'        
        status_code  =  response.status_code    
        
        if json_content and status_code in range(199 ,300) :
            return await response.json()  
        
        else:
            if "Ensure this field" in str(await response.text()):
                await append_link(data['url'])
            else:
                print(await response.json() if json_content else await response.content)
    
class  JsToDIct:
    def __init__(self, url : str, CACHE_DIR : Path = BASE_DIR / "cache") -> None:
        self.url =  url
        self.hashed_url =  self.hash_url(self.url)
        self.init_dirs(CACHE_DIR=CACHE_DIR)
        self.node = 'node'
        self.window_obj = "window.__NUXT__"        


    def init_dirs(self,CACHE_DIR):
        self.cache_dir  =  CACHE_DIR
        self.js_dir  = Path(CACHE_DIR , "js")
        self.json_dir  =  Path(CACHE_DIR,  "json") 
        self.cache_dir.mkdir(exist_ok=True)
        self.js_dir.mkdir(exist_ok=True)
        self.json_dir.mkdir(exist_ok=True)
        self.js_file = Path( self.js_dir , self.hashed_url + ".js")
        self.json_file  =   Path(self.json_dir,  self.hashed_url  + ".json")
        
        
    def hash_url(self, url : str) -> hashlib.md5 :
        return hashlib.md5(url.encode('utf-8')).hexdigest()


    def clean_content(self,content : str):
        return content.replace(self.window_obj,"const result")
    
    def excute_js(self,):
        subprocess.run([self.node, self.js_file])
        
    def write_js(self,content : str):
        with open(self.js_file,"w" ,encoding="utf-8") as f:
            f.write(
"""const fs = require('fs');
%s
fs.writeFileSync(%s, JSON.stringify(result, null, 2));
""" % (self.clean_content(content), repr(str(self.json_file)) )
            )

    def decode(self,content : str,):
        self.write_js(content)
        self.excute_js()
        with open(self.json_file, 'r',encoding="utf-8") as f:
            return json.load(f)
        
    

# class  JsToDIct:
#     def __init__(self, content ) -> None:
#         self.content = content
#         self.output = BASE_DIR / 'output.json'
#         self.js_file = BASE_DIR / 'excute.js'
#         self.node = 'node'
#         self.window_obj = "window.__NUXT__"        
#         self.clean_content()

#     def clean_content(self,):
#         return  self.content.replace(self.window_obj,"const result")
    
#     def excute_js(self,):
#         subprocess.run([self.node, self.js_file])
        
#     def write_js(self,):
#         with open(self.js_file,"w" ,encoding="utf-8") as f:
#             f.write(
# """const fs = require('fs');
# %s
# fs.writeFileSync(%s, JSON.stringify(result, null, 2));
# """ % (self.clean_content(), repr(str(self.output)) )
#             )

#     def decode(self,):
#         self.write_js()
#         self.excute_js()
#         with open(self.output, 'r',encoding="utf-8") as f:
#             return json.load(f)
        
        
        

class Helper:
    def __init__(self, 
            location_id : int = None,
            area : str =  None,
            location_geo : str = None,
            db_store : bool =  False,
            max_concurrent_tasks: int = 5
                 ) -> None:
        
        self.location_id =  location_id
        self.area =  area
        self.location_geo =  location_geo
        
        self.max_concurrent_tasks = max_concurrent_tasks
        
        self.session  =  AsyncSession()
        self.apipsaver =  APISAVER()
        
        self.working_dir =  BASE_DIR / str(datetime.now().strftime("%d-%m-%Y"))
        self.working_dir.mkdir(exist_ok=True)
            
            
        self.db_store =  db_store
        self.base_url = "https://booksy.com"
        self.page_url = "https://booksy.com/en-us/s"
        self.api_url = "https://us.booksy.com/core/v2/customer_api/businesses/"
        
        self.file_name : str  = "booksy.csv"
        

    def make_params(self, page):
        data  = {
            "no_thumbs":True,
            "with_markdown":1,
            "businesses_page":page,
            "include_ext_listing":0,
            "include_venues":1,
            "include_seo_metadata":1,
            "include_b_listing":1,
            "include_details":1,
            "include_treatment_services":1,
            "response_type":"listing_web"
            }
        
        if  self.location_id :
            data.update(
                {
                    "location_id":self.location_id,
                    "area":self.area,
                    "location_geo":self.location_geo,
                }
            )
        
        return data


    def json_path(self, data , keys :  list , default  = None):
        try:return reduce(lambda d, key: d[key], keys, data)
        except:return default
           
    def to_csv(self,data,file_name : str):
        abs_path = os.path.join(self.working_dir ,file_name)
        df =pd.json_normalize(data)
        df.to_csv(abs_path,mode="a",header=not os.path.exists(abs_path),index=False)      
                
    async def run_in_batches(self, tasks):
        """
        Runs tasks in batches of the specified size and collects their results.

        Returns:
            List of results returned by the tasks.
        """
        results = []
        for i in range(0, len(tasks), self.max_concurrent_tasks):
            batch = tasks[i:i + self.max_concurrent_tasks]
            print(f"Running batch {i // self.max_concurrent_tasks + 1}: {len(batch)} tasks")
            batch_results = await asyncio.gather(*batch)
            results.extend(batch_results)
            print(f"Finished batch {i // self.max_concurrent_tasks + 1}")
        
        return results
    
    async def get_script_data(self, soup : BeautifulSoup ):
        content  = soup.select_one('script:-soup-contains("window.__NUXT__")')
        return content.get_text(strip=True) if content else logger.critical(f"no script")
    
    async def fetch(self, url: str, json_response: bool = False,sleep  = None, **kwargs):
        if sleep :
            time.sleep(sleep)
            
        retry = 1    
        
        while retry >= 0:
            try:
                response = await self.session.get(
                                                    url,headers=HEADERS,verify=False,
                                                    impersonate="chrome",**kwargs
                                                    )
                if response.status_code == 200:
                    return  response.json() if json_response else BeautifulSoup( response.content, "html.parser")
                
                elif response.status_code == 403:
                    await append_link(url)
                    print("u got blocked sleeping for 60  seconds")
                    time.sleep(60)
                    
                else: 
                    await append_link(url)
                    print(url , " => ", response.status_code,  response.text) if not json_response  else  None
            except Exception as e:
                await append_link(url)
                print(f"{url} -> {e}")

            retry -= 1


    async def url_handler(self, url : str ):
        print("handling : ",url)
        soup = await  self.fetch(url,sleep=1)
        if not soup:
            logger.warning(f"{url} without soup.")
            return
        script_data  = await  self.get_script_data(soup)
        json_d = JsToDIct(url).decode(script_data) 
        
        if not json_d: 
            logger.warning(f"{url} could not decode js script content")            
            return

        state  = self.json_path(json_d,["state"])
        business =  self.json_path(state,["business"])
        business_info =  self.json_path(business,["business"])
            
        country, state_name, county = "","",""
        
        for v  in  self.json_path(business_info,["regions"],default=[]):
            r_type =  v.get("type")
            full_name =  v.get("full_name")
            if r_type == "country":
                country = full_name
            elif  r_type == "state":
                state_name =  full_name
            elif r_type == "county":
                county =  full_name
        
        data =   {
            "url":url,
            "book_id":self.json_path(business_info,["id"]),
            "name":self.json_path(business_info,["name"]),
            "subdomain":self.json_path(business_info,["subdomain"]),
            "description":self.json_path(business_info,["description"]),
            "categories":self.json_path(business_info,["business_categories",0,"name"]),
            
            "country":country,
            "state":state_name,
            "county":county,
            "address":self.json_path(business_info,["location","address"]),
            "city":self.json_path(business_info,["location","city"]),            
            "latitude":self.json_path(business_info,["location","coordinate","latitude"]),
            "longitude":self.json_path(business_info,["location","coordinate","longitude"]),
            
            "website":self.json_path(business_info,["website"]),
            "phone":self.json_path(business_info,["phone"]),
            "facebook":self.json_path(business_info,["facebook_link"]),
            "email":self.json_path(business_info,["public_email"]),
            "instagram":self.json_path(business_info,["instagram_link"]),
        }
        
        if self.db_store:
            await self.apipsaver.create(
                    data=data or  {"url":url}
                )

        return data

    async def parse_page(self, soup : dict):
        return [self.base_url + "/en-us/" +  self.json_path(item,["url"]) for item in  
                self.json_path(soup,["businesses"],default=[])] if soup else []
    
    async def page_handler(self, page: int):
        url_params = self.make_params(page)
        page_data = await self.fetch(self.api_url, json_response=True, params=url_params)
        urls = await  self.parse_page(page_data)
        tasks = [self.url_handler(url) for url in urls] 
        await self.run_in_batches(tasks)
        

        return len(urls) > 0
    
    # def page_handler(self,page  : int):
    #     url_params =  self.make_params(page)
        
    #     page_soup  =  self.fetch(self.api_url,params=url_params,json=True)
        
    #     urls =   self.parse_page(page_soup)
        
    #     for url in urls:
    #         data  = self.url_handler(url)
    #         if self.db_store:
    #             self.apipsaver.create(
    #                 data=data or  {"url":url}
    #             )
                
    #         # self.to_csv({"name":data["name"],
    #         #              "phone":data["phone"],
    #         #              "url":data["url"]
    #         # },
    #         # file_name=self.file_name)
            
    #     return len(urls) > 0

    # def run(self ):
            
    #     index = 281
    #     keep = True
    #     while keep:
    #         print(f"checking page {index}")
    #         keep =  self.page_handler(index)
    #         index += 1
        
    
    async def run(self):
        index = 461
        keep = True
        while keep:
            print(f"Checking page {index}")
            keep = await self.page_handler(index)
            index += 1
    
    
if __name__ == "__main__":
    helper = Helper(db_store=True, max_concurrent_tasks=2)
    asyncio.run(helper.run())
    # app =  Helper(
    #     # location_id=135315,
    #     # area="37.83214,-122.34945,37.60402,-122.51429",
    #     # location_geo="37.77712,-122.41966",
    #     db_store=True
    # )
    
    # app.run()
    # # url = "https://booksy.com/en-us/1308411_harry-barber_barber-shop_134715_san-francisco#ba_s=sr_1"
    # # output  = app.url_handler(url)
    # # print(output)