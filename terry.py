import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import date
from read_json_urls import read_domains_json_list
# from data_frames import first_dataframe, second_dataframe, Third_dataframe , Fourth_dataframe


url = "https://www.byterry.com/"


def main():

    urls = read_domains_json_list()
    for url in urls:
        soup = BeautifulSoup(requests.get(url).content, "html.parser")
        multiple_prod_links = soup.find_all('a',{'class':'productBlock_link'})
        product_links = ["https://www.byterry.com"+i.get('href') for i in multiple_prod_links]
        final_new_menu = list(dict.fromkeys(product_links))
        Category_tag = soup.find("h1",{'id':'responsive-product-list-title'}).getText().strip()
        Category_url = url

        # final loop to handle single pages
        for link in final_new_menu:
            soup = BeautifulSoup(requests.get(link).content, "html.parser")
            Url = "https://www.byterry.com/"+(link.split('/')[-1])
            

            Brand_name = soup.find('div',{'class':'westendHeader_logo-desktop'}).getText().strip()
            Product_name = soup.find('h1',{'class':'productName_title'}).getText().strip()
            Descriptions = soup.find('div',{'class':'productDescription_synopsisContent'}).getText().strip()
            try:
                Ingredients = soup.find_all('div', {'id':'product-description-content-lg-7'})[-1].getText().strip()
            except:
                Ingredients = "NA"
            try:
                product_size = soup.find('ul',{'class':'productDescription_contentPropertyValue productDescription_contentPropertyValue_volume'}).getText().strip()
            except:
                product_size = "NA"
                
            Price = soup.find('p',{'class':'productPrice_price'}).getText().strip()
            Variant_Name = "NA"

            if Price == "NA":
                sale = "N"
                sale_price = "NA"
                sold_out = "N"
            else:
                sale = "N"
                sale_price = "NA"
                sold_out = "N"
            
            Date = date.today()
            
            with open('url_conatiner.txt') as file:
                content = file.read().splitlines()
                if Url not in content:
                    first_dataframe(Url,Brand_name,Product_name,Date)
                    second_dataframe(Url,Brand_name,Category_tag,Category_url,Brand_name,Date)
                    Third_dataframe(Category_tag,Category_url,Brand_name,Date)
                    Fourth_dataframe(Url,Brand_name,Product_name,Ingredients,
                                    Price,sale,sale_price,Descriptions,
                                sold_out,product_size,Variant_Name,Date)
                    
                    # append new url in text file
                    with open('url_conatiner.txt', 'a+') as f:
                        f.write("%s\n" % Url)



def first_dataframe(Url,Brand_name,Product_name,Date):
    c1   =   pd.Series(Url,name="Url")
    c2   =   pd.Series(Brand_name,name="Brand Name")
    c3   =   pd.Series(Product_name,name="Product Name")
    c4   =   pd.Series(Date,name="Scrape Date")
    data =   pd.concat([c1,c2,c3,c4], axis=1)

    data_frame = pd.DataFrame(data)
    path = os.path.exists('Terry UK_product_table.csv')
    
    if path:
        data_frame.to_csv('Terry UK_product_table.csv',index=False,mode='a',header=False, sep =',')
    else:
        data_frame.to_csv('Terry UK_product_table.csv',index=False,mode='a',header=True, sep =',')

    print("Data inserted in dataframe successfully")


def second_dataframe(Url,Product_name,Category_tag,Category_url,Brand_name,Date):
    c1   =   pd.Series(Url,name="Url")
    c2   =   pd.Series(Product_name,name="Name")
    c3   =   pd.Series(Category_tag,name="Category tag")
    c4   =   pd.Series(Category_url,name="Category url")
    c5   =   pd.Series(Brand_name,name="Brand name")
    c6   =   pd.Series(Date,name="Scrape Date")
    data =   pd.concat([c1,c2,c3,c4,c5,c6], axis=1)

    data_frame = pd.DataFrame(data)
    path = os.path.exists('Terry UK_category_product_table.csv')

    if path:
        data_frame.to_csv('Terry UK_category_product_table.csv',index=False,mode='a',header=False, sep =',')
    else:
        data_frame.to_csv('Terry UK_category_product_table.csv',index=False,mode='a',header=True, sep =',')
    
    print("Data inserted in dataframe successfully")



def Third_dataframe(Category_tag,Category_url,Brand_name,Date):
    c3   =   pd.Series(Category_tag,name="Category tag")
    c4   =   pd.Series(Category_url,name="Category url")
    c5   =   pd.Series(Brand_name,name="Brand name")
    c6   =   pd.Series(Date,name="Scrape Date")
    data =   pd.concat([c3,c4,c5,c6], axis=1)

    data_frame = pd.DataFrame(data)
    path = os.path.exists('Terry UK_category_table.csv')

    if path:
        data_frame.to_csv('Terry UK_category_table.csv',index=False,mode='a',header=False, sep =',')
    else:
        data_frame.to_csv('Terry UK_category_table.csv',index=False,mode='a',header=True, sep =',')
    
    print("Data inserted in dataframe successfully")





def Fourth_dataframe(Url,Brand_name,Product_name,Ingredients,
                        Price,sale,sale_price,Descriptions,
                        sold_out,product_size,Variant_Name,Date):

    c1   =   pd.Series(Url,name="Url")
    c2   =   pd.Series(Brand_name,name="Brand name")
    c3   =   pd.Series(Product_name,name="Product Name")
    c4   =   pd.Series(Ingredients,name="Ingredients")
    c5   =   pd.Series(Price,name="Price")
    c6   =   pd.Series(sale,name="sale")
    c7   =   pd.Series(sale_price,name="sale Price")
    c8   =   pd.Series(Descriptions,name="Descriptions")
    c9   =   pd.Series(sold_out,name="sold out")
    c10  =   pd.Series(product_size,name="product size")
    c11  =   pd.Series(Variant_Name,name="Variant Name")
    c12  =   pd.Series(Date,name="Date")


    data = pd.concat([c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12], axis=1)
    data_frame = pd.DataFrame(data)

    path = os.path.exists('Terry UK_product_details_table.csv')
    if path:
        data_frame.to_csv('Terry UK_product_details_table.csv',index=False,mode='a',header=False, sep =',')
    else:
        data_frame.to_csv('Terry UK_product_details_table.csv',index=False,mode='a',header=True, sep =',')
    
    print("Data inserted in dataframe successfully")



main()