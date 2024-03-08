import json
import requests
from datetime import datetime, timezone, timedelta

import pandas as pd
from parsel import Selector

def get_product_info(json_url, base_url):
    """Returns information on products/variants from given Shopify store.
    
       Args:
           json_url: str, url to store's products.json file
           base_url: str, base url for all store products
    """
    # Pulling initial product info from products.json
    products = requests.get(json_url).json()['products']
    
    # Putting product data into lists
    ids = [item['id'] for item in products]
    titles = [item['title'] for item in products]
    urls= [base_url + item['handle'] for item in products]
    descs = [item['body_html'] for item in products]
    created_ats = [item['created_at'] for item in products]
    published_ats = [item['published_at'] for item in products]
    updated_ats = [item['updated_at'] for item in products]
    types = [item['product_type'] for item in products]
    tags = [item['tags'] for item in products]
    variants = [item['variants'] for item in products]
    
    eastern = timezone(timedelta(hours=-5))
    now = [datetime.now(tz=eastern).replace(microsecond=0).isoformat()]
    data_pull_datetime = now * len(ids)
    
    # Combining into list of product dictionaries
    products_data = [{'data_pulled_at': p_now,
                      'product_id': p_id,
                      'product_title': p_title,
                      'product_url': p_url,
                      'product_description': p_desc,
                      'product_created_at': p_created,
                      'product_published_at': p_published,
                      'product_updated_at': p_updated,
                      'product_type': p_type,
                      'product_tags': p_tags,
                      'product_variants': p_variants} 
                     for p_now, p_id, p_title, p_url, p_desc, p_created, p_published, p_updated, p_type, p_tags, p_variants 
                     in zip(data_pull_datetime, ids, titles, urls, descs, created_ats, published_ats, updated_ats, types, tags, variants)]
    
    # Cleaning variant data
    for product in products_data:
        existing_variants = product['product_variants']
        
        # Removing unwanted keys from variant dictionaries
        unwanted_keys= ['option1', 'option2', 'option3', 'featured_image', 'product_id', 'compare_at_price']
        for variant in existing_variants:
            [variant.pop(key) for key in unwanted_keys]
            
        # Pulling inventory data from webpage via parsel
        item_page = requests.get(product['product_url']).text
        selector = Selector(text=item_page)
        script = selector.xpath('//head/script[@type="text/javascript"]/text()').get()
        raw_variants = script[script.find('[\n  \n    {\n      "fixed_date"'):script.find(''',\n  "collections"''')]
        variants = json.loads(raw_variants)
        inventory = [{'inventory_quantity': item['inventory_quantity'], 
                      'inventory_management': item['inventory_management'],
                      'inventory_policy': item['inventory_policy']} for item in variants]
    
        # Changing key names in dictionaries
        initial_variants = [x | y for x, y in zip(existing_variants, inventory)]
        variant_keys = ['variant_id', 'variant_title', 'variant_sku', 'variant_requires_shipping', 'variant_taxable', 
                        'variant_available', 'variant_price', 'variant_grams', 'variant_position', 'variant_created_at', 
                        'variant_updated_at', 'variant_inventory_quantity', 'variant_inventory_management', 'variant_inventory_policy']
        new_variants = []
        for item in initial_variants:
            new_item = dict(zip(variant_keys, item.values()))
            new_variants.append(new_item)
            
        #Replacing with new variant list of dicts     
        product['product_variants'] = new_variants
    
    # Converting to pandas df to expanding variant data
    raw_df = pd.json_normalize(data=products_data, record_path='product_variants', meta=['data_pulled_at',
                                                                                         'product_id', 
                                                                                         'product_title', 
                                                                                         'product_url', 
                                                                                         'product_description', 
                                                                                         'product_created_at', 
                                                                                         'product_published_at', 
                                                                                         'product_updated_at', 
                                                                                         'product_type', 
                                                                                         'product_tags'])
    
    # Reordering df columns
    df = raw_df.reindex(columns=['data_pulled_at', 'product_id', 'product_title', 'product_url', 'product_description', 'product_created_at', 
                                 'product_published_at', 'product_updated_at', 'product_type', 'product_tags', 'variant_id', 
                                 'variant_title', 'variant_sku', 'variant_requires_shipping', 'variant_taxable', 'variant_available', 
                                 'variant_price', 'variant_grams', 'variant_position', 'variant_created_at', 'variant_updated_at', 
                                 'variant_inventory_quantity', 'variant_inventory_management', 'variant_inventory_policy'])
    
    return df