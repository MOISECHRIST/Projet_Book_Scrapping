import sys
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re
import pandas as pd
from  loguru import logger

logger.remove()
logger.add(f"books_warning.log",
           level="WARNING",
           rotation="500mb")

logger.add(sys.stderr, level="SUCCESS")
logger.add(sys.stderr, level="WARNING")

BASE_URL="https://books.toscrape.com/index.html"
STAR={"One":1,"Two":2,"Three":3,"Four":4,"Five":5}

def get_article_detail(response):
    soup=BeautifulSoup(response.text, "html.parser")

    detail_div=soup.find("div",class_="col-sm-6 product_main")
    
    stock_node=detail_div.find("p",class_="instock availability")
    if stock_node:
        stock=int(re.findall(r"[0-9]+",stock_node.text)[0])
    else :
        stock=0
        logger.error("Aucun noeud contenant les etoiles n'a ete trouve")
        
    

    star_node=detail_div.find("i", class_="icon-star")
    star_node = star_node.parent
    if star_node:
        star=STAR[star_node.get("class")[-1]]
    else :
        logger.error("Aucun noeud contenant les etoiles n'a ete trouve")
        star=0
    
    head_div=soup.find("ul", class_="breadcrumb").find_all("li")
    try:
        category=head_div[2].text.strip()
        category_link=head_div[2].find("a").get("href")
        category_link=category_link.replace("..","https://books.toscrape.com/catalogue")
    except IndexError as e:
        logger.error("Aucune categorie n'a ete trouve")
        category=None
        category_link=None
    
    return (stock, star, category, category_link)

def collect_articles_on_page(response, session):

    soup=BeautifulSoup(response.text, 'html.parser')

    articles = soup.find_all('article', class_='product_pod')
    all_data=[]
    for article in articles:
        if article.find('h3').find('a').get('title'):
            data={}

            data["title"]=article.find('h3').find('a').get('title')

            
            price=article.find('div', class_="product_price").find('p').text
            try:
                price=re.findall(r"[0-9.]+",price)[0]
            except IndexError as e :
                logger.error("Aucun nombre n'a ete trouve")
                price = 0.0
                
            data["price"]=float(price)

            url=article.find('h3').find('a').get('href')
            if "catalogue" in url:
                data["link"]=urljoin(BASE_URL, url)
            else:
                data["link"]=urljoin("https://books.toscrape.com/catalogue/", url)
            
            try:
                response=session.get(data["link"])
                response.raise_for_status()
                data["stock"], data["star"], data["category"], data["category_url"]=get_article_detail(response)
            except requests.exceptions.RequestException as e:
                logger.error(f"Erreur lors de la requete HTTP : {e}")
                (data["stock"], data["star"], data["category"], data["category_url"])=(0,0,"","")
            logger.success(f"Enregistrement de l'article {data['title']}")
            all_data.append(data)
        else : 
            logger.error("Aucun noeud contenant le titre de l'article n'a ete trouve")
    
    return all_data

def get_next_url(response):
    soup=BeautifulSoup(response.text, 'html.parser')    
    next_page_node = soup.find("ul", class_="pager").find("li", class_="next")
    if next_page_node:
        next_url=next_page_node.find("a").get("href")
        if "catalogue" in next_url:
            next_url=urljoin(BASE_URL,next_url)
        else:
            next_url=urljoin("https://books.toscrape.com/catalogue/", next_url)
        return next_url
    else:
        logger.error("Aucune url pour la page suivante n'a ete trouvee")
        return None

def get_all_urls(session, url=BASE_URL):
    all_articles=[]
    try:
        response= session.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur lors de la requete HTTP : {e}")
        return all_articles
    while url:
        response= session.get(url)
        articles_in_on_page=collect_articles_on_page(response, session)
        all_articles.extend(articles_in_on_page)
        url=get_next_url(response)
    return all_articles


def main():
    with requests.Session() as session:
        all_articles=get_all_urls(session)           
    
    df=pd.DataFrame(all_articles)
    df.to_csv("scrapping_data.csv", index=False)


if __name__=="__main__":
    main()