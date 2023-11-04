import requests
from bs4 import BeautifulSoup

url = 'https://en.pathe.nl/films/actueel'

response = requests.get(url)

if response.status_code == 200:
    html = response.text
    print(html)
    print('Success!')
else:
    print('An error has occurred.')

soup = BeautifulSoup(html, 'html.parser')

movie_titles = [title.text for title in soup.find_all('div', class_='poster__footer')]

print(movie_titles)

movie_titles = [title.strip() for title in movie_titles]

print(movie_titles)



