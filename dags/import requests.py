import requests
fake_products = requests.get('https://fakestoreapi.com/products').json()




list_yeni =  []

for product in fake_products:
    tuple_hali = (product['id'], product['title'],product['price'])
    list_yeni.append(tuple_hali)




print(list_yeni[0])
print(len(list_yeni))