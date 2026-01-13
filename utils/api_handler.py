import requests
import os
from typing import List, Dict, Any

def fetch_all_products() -> List[Dict[str, Any]]:
    url = "https://dummyjson.com/products?limit=100"
    print("Fetching products from API...")
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        products = r.json().get('products', [])
        print(f"Success: {len(products)} products fetched")
        return products
    except Exception as e:
        print(f"API error: {e}")
        return []

def create_product_mapping(products: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    mapping = {}
    for p in products:
        pid = p.get('id')
        if pid:
            mapping[pid] = {
                'title': p.get('title', ''),
                'category': p.get('category', ''),
                'brand': p.get('brand', ''),
                'rating': p.get('rating')
            }
    return mapping

def enrich_sales_data(
    transactions: List[Dict[str, Any]],
    mapping: Dict[int, Dict[str, Any]]
) -> List[Dict[str, Any]]:
    enriched = []
    for t in transactions:
        row = t.copy()
        pid_str = t.get('ProductID', '')
        try:
            num_id = int(pid_str.lstrip('P'))
        except:
            num_id = None
        if num_id and num_id in mapping:
            info = mapping[num_id]
            row['API_Category'] = info['category']
            row['API_Brand'] = info['brand']
            row['API_Rating'] = info['rating']
            row['API_Match'] = True
        else:
            row['API_Category'] = None
            row['API_Brand'] = None
            row['API_Rating'] = None
            row['API_Match'] = False
        enriched.append(row)
    return enriched

def save_enriched_data(
    enriched: List[Dict[str, Any]],
    filename: str = "data/enriched_sales_data.txt"
):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    headers = [
        'TransactionID', 'Date', 'ProductID', 'ProductName',
        'Quantity', 'UnitPrice', 'CustomerID', 'Region',
        'API_Category', 'API_Brand', 'API_Rating', 'API_Match'
    ]
    with open(filename, 'w', encoding='utf-8') as f:
        f.write('|'.join(headers) + '\n')
        for row in enriched:
            values = []
            for h in headers:
                v = row.get(h)
                if v is None:
                    values.append('')
                elif isinstance(v, bool):
                    values.append(str(v).lower())
                else:
                    values.append(str(v).replace('|', '\\|'))
            f.write('|'.join(values) + '\n')
    print(f"Saved enriched data: {filename}")
