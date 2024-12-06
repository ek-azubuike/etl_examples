import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

# define known entities
url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name = 'Movies.db'
table_name = 'Top_50'
csv_path = './top_50_films.csv'
df = pd.DataFrame(columns = ["Average Rank",
                             "Film",
                             "Year",
                             "Rotten Tomatoes' Top 100"])
count = 0

# loading webpage for scraping
html_page = requests.get(url).text
html_data = BeautifulSoup(html_page, "html.parser")

# scrape data
tables = html_data.find_all('tbody')
rows = tables[0].find_all('tr')

for row in rows:
    if count < 50:
        col = row.find_all('td')
        if len(col) != 0:
            movie_dict = {"Average Rank" : int(col[0].contents[0]),
                          "Film" : str(col[1].contents[0]),
                          "Year" : int(col[2].contents[0]),
                          "Rotten Tomatoes' Top 100" : str(col[3].contents[0])}
            df1 = pd.DataFrame(movie_dict, index = [0])
            df = pd.concat([df, df1], ignore_index = True)
            count += 1
        elif len(col) == 0:
            continue
        else:
            break

print(df[df.Year > 2000])

# load data to CSV
df.to_csv(csv_path)

# load CSV to DB
conn = sqlite3.connect(db_name)
df.to_sql(table_name,
          conn,
          if_exists = 'replace',
          index = False)
conn.close()