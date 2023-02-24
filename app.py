import requests
import logging as log
from flask import Flask, request, render_template
from flask_cors import CORS, cross_origin
from bs4 import BeautifulSoup
from threading import Thread, current_thread
import pymongo
from dotenv import load_dotenv
from os import getenv
log.basicConfig(filename='scrapper.log', level=log.INFO )

load_dotenv()

app = Flask(__name__)




# connect with the database
try :
    client = pymongo.MongoClient( getenv('DATABASE_URL') )
except Exception as e :
    log.error(e)

else :
    log.info('db connection successfull')
    db = client['flipcart-review-scrapper']
    coll = db['products']






def base_url() : return 'https://www.flipkart.com'


# if query is not given :
#   1.  try to fetch source code as text
#   2.  after getting source code successfully convert it as Beautifulsoup object and return it
def fetch_web_page( url : str, *, query : str = None, page_num : int = 1 ) :
    '''try to fetch the web source and convert it to Beautifulsoup Object'''
    
    search_url = f"{url}/search?q={query}&page={page_num}"

    try :
        client_res = requests.get(url= search_url if query else url)
    except Exception as e :
        log.error(e)
    else :
        log.info('successfully get source code of the page')
        return BeautifulSoup( client_res.text, "html.parser" )




# try to find all links in the given page 
#   1.  find every 'a' tag
#   2.  get the 'href' inside the a tag
# yield every link one by one after successfully getting it
# combine with base url
def get_product_link( page ) :
    try :
        product_links = page.find_all('a', {'class' : '_1fQZEK'})
    except Exception as e :
        log.error(e)
    else :
        log.info('successfully get products links')
        for link in product_links :
            yield  base_url() + link['href']


class ReviewThreads( Thread ) :
    def __init__(self, review_div) -> None :
        super().__init__()
        self.comment = review_div
        self.review = {}

    def run(self) -> None:

        try :
            review = { 
                "name" : self.comment.find('p', {'class' : '_2sc7ZR _2V5EHH'}).text,
                "in_short" : self.comment.find('p', {'class' : '_2-N8zT'}).text,
                "description" : self.comment.find('div', {'class' : 't-ZTKy'}).text.replace('READ MORE', ''),
                "ratings" : self.comment.find('div', {'class' : '_3LWZlK _1BLPMq'}).text
            }
        except Exception as e :
            log.error(e)
        
        else :
            log.info('successfully get review')
            self.review = review



class ProductThreads( Thread ) :
    def __init__(self, url) -> None:
        super().__init__()
        self.url = url
        self.product_name = ''
        self.reviews = []
    


    def run(self) -> None:
        page = fetch_web_page(self.url)

        self.reviews += self.get_reviews( page )
        self.product_name = self.get_product_name( page )
    


    
    # fetch the product name displaing on the top of the page
    def get_product_name( self, page ) :
        try :
            product_name = page.find('span', {'class' : 'B_NuCI'}).text
        except Exception as e :
            log.error(e)
        else :
            log.info('successfully get product name')
            return product_name



    def get_reviews( self, page) :
        '''create separate threads to get all reviews'''

        comments = page.find_all('div', {'class' : 'col _2wzgFH'})
        
        comment_threads = []
        reviews = []

        for comment in comments :
            new_thread = ReviewThreads(comment)
            comment_threads.append( new_thread )
            new_thread.start()
        
        for t in comment_threads :
            t.join()
            reviews.append( t.review )
        
        return reviews




def fetch_from_db(query : dict) -> list :
    try :
        products = list( coll.find(query, {'_id' : 0, 'slug' : 0, 'product' : 0}) )
    except Exception as e :
        log.error(e)
    else :
        log.info('successfully get data from the database')
    
    return products if products else None




def store_to_db( documents : list[dict] ) -> bool :
    try :
        coll.insert_many(documents=documents)
        
    except Exception as e :
        log.error(e)
        return False

    else :
        log.info('successfully added in the database')
        return True





def scrapper( query : str ) -> list :
    '''scrap the data from flipkart using multi-thread'''
    try :
        page = fetch_web_page( base_url(), query=query ) #this is the main page containing all products
        product_links = get_product_link(page) # get list of all links available in main page
    except Exception as e :
        log.error(e)
    
    else :
        log.info('successfully get all products links')
        products = []
        collection = []
        threads = []


        # using threads to work on respective product's page simontenously
        for link in product_links :
            new_thread = ProductThreads(link)
            threads.append( new_thread )
            new_thread.start()

        # wait and connect with MainThread
        for t in threads : 
            t.join()
            products += [{t.product_name : t.reviews}]
            collection += [{ t.product_name : t.reviews, 'slug' : query, 'product' : t.product_name }]
        
        return products, collection
    
    return None
    




@app.route('/', methods=['GET', 'POST'])
@cross_origin()
def search() :
    if request.method == 'POST' :
        query = request.form.get('query').replace(' ', '+')


        # try to fetch from database
        # if found then return to the template
        products = fetch_from_db({'slug' : query})
        if products : return render_template('index.html', products=products)
        

        # else if not found in database
        # scrap from the flipcart
        products, collection = scrapper(query=query) #returns a list


        # store the all products to the coll
        db_store_thread = Thread(target=store_to_db, args=(collection,))
        db_store_thread.start()

        return render_template('index.html', products=products, total_products=len(products))
    
    return render_template('index.html')





if __name__ == '__main__' :
    app.run( host='localhost', port=8008 )
    client.close()