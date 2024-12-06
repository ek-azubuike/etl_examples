library(tidyverse)
library(dslabs)

url <- "https://archive.ics.uci.edu/ml/machine-learning-databases/breast-cancer-wisconsin/wdbc.data"
read_lines(url, n_max = 10)
dat <- read_csv(url, col_names = FALSE)
str(dat)

str(co2)

co2_wide <- data.frame(matrix(co2, ncol = 12, byrow = TRUE)) %>% 
  setNames(1:12) %>%
  mutate(year = as.character(1959:1997))

str(co2_wide)
head(co2_wide)

co2_tidy <- co2_wide %>% 
  pivot_longer(-year, names_to = "month", values_to = "co2")

co2_tidy %>% 
  ggplot(aes(as.numeric(month),
             co2,
             color = year)) +
  geom_line()

data(admissions)
dat <- admissions %>% select(-applicants)
head(dat)

dat %>% pivot_wider(names_from = gender,
                    values_from = admitted)

tmp <- admissions %>%
  pivot_longer(cols = c(admitted, applicants), names_to = "key", values_to = "value")
tmp

tmp %>% 
  unite(col = "column_name", c(key, gender))

df1 <- data.frame(x = c("a", "b"), y = c("a", "a"))
df2 <- data.frame(x = c("a", "a"), y = c("a", "b"))

setdiff(df1, df2)

library(Lahman)
top <- Batting %>% 
  filter(yearID == 2016) %>%
  arrange(desc(HR)) %>% 
  slice(1:10)

top %>% as_tibble()

People %>% as_tibble()

top_name <- top %>% 
  left_join(People) %>%
  select(playerID, nameFirst, nameLast, HR) %>% 
  as_tibble()

Salaries %>%
  filter(yearID == 2016) %>% 
  right_join(top_name) %>% 
  select(playerID, nameFirst, nameLast, HR)

awarded <- AwardsPlayers %>%
  filter(yearID == 2016)

length(setdiff(awarded$playerID, top$playerID))

length(intersect(awarded$playerID, top$playerID))

### Web Scraping
library(rvest)
url <- "https://web.archive.org/web/20181024132313/http://www.stevetheump.com/Payrolls.htm"
h <- read_html(url)
nodes <- h %>% html_nodes("table")
html_table(nodes[[8]])

dfs <- list()

for (i in 1:4) {
  dfs[[i]] <- html_table(nodes[[i]])
}

str(dfs)
dfs[[1]]

tab_1 <- html_table(nodes[[10]]) %>% 
  select(-X1) %>% 
  slice(-1) %>% 
  setNames(c("Team", "Payroll", "Average"))
  
tab_2 <- html_table(nodes[[19]]) %>% 
  slice(-1) %>% 
  setNames(c("Team", "Payroll", "Average"))

full_tab <- full_join(tab_1, 
                      tab_2, 
                      by = join_by(Team))
# Brexit data
url <- "https://en.wikipedia.org/w/index.php?title=Opinion_polling_for_the_United_Kingdom_European_Union_membership_referendum&oldid=896735054"
h <- read_html(url)
tab <- h %>% html_nodes("table")
length(tab)

### String Processing
url <- "https://en.wikipedia.org/w/index.php?title=Opinion_polling_for_the_United_Kingdom_European_Union_membership_referendum&oldid=896735054"
tab <- read_html(url) %>% html_nodes("table")
polls <- tab[[6]] %>% html_table(fill = TRUE)

polls <- polls %>% 
  setNames(c("dates", "remain", "leave", "undecided", "lead",
             "samplesize", "pollster", "poll_type", "notes"))

polls <- polls[str_detect(polls$remain, "%$"),]
polls

# convert percentages to proportions between 0 and 1
parse_number(polls$remain) / 100
as.numeric(str_replace(polls$remain, "%", ""))/100

polls$undecided %>% 
  str_replace("N/A", "0")

"\\d{1,2}\\s[a-zA-Z]{3,5}"

### Dates, Times, Text Mining
options(digits = 3)
data("brexit_polls")

# How many polls had a start date (startdate) in April (month number 4)?
brexit_polls %>% 
  mutate(month = month(startdate)) %>% 
  count(month)

# Use the round_date() function on the enddate column with the argument 
# unit="week". How many polls ended the week of 2016-06-12?
brexit_polls %>% 
  mutate(week = round_date(enddate, unit = "week")) %>% 
  count(week) %>% 
  filter(week == "2016-06-12")

brexit_polls %>% 
  mutate(weekday = weekdays(enddate)) %>% 
  count(weekday)

data("movielens")
str(movielens)
head(movielens)

movielens <- movielens %>% 
  mutate(dt = as_datetime(timestamp))

movielens[1:10,]

# Which year had the most movie reviews?
movielens %>% 
  mutate(year = year(dt)) %>% 
  count(year) %>% 
  arrange(desc(n))

# Which hour of the day had the most movie reviews?
movielens %>% 
  mutate(hour = hour(dt)) %>% 
  count(hour) %>% 
  arrange(desc(n))

library(tidyverse)
library(tidytext)
library(gutenbergr)
options(digits = 3)

gutenberg_metadata

gutenberg_metadata %>% 
  filter(str_detect(title, "^Pride and Prejudice$")) %>% 
  select(gutenberg_id, title)

# pull book id
id <- gutenberg_works(title == "Pride and Prejudice")$gutenberg_id
mirror <- "http://mirror.csclub.uwaterloo.ca/gutenberg/"

# download book
pnp <- gutenberg_download(gutenberg_id = id, mirror = mirror)

# tokenize words
words <- pnp %>% 
  unnest_tokens(output = "word",
                input = "text",
                token = "words")
# remove stop words
words <- words %>% 
  filter(!word %in% stop_words$word)

# filter tokens containing digits
words <- words %>% 
  filter(str_detect(word, "\\d+", negate = TRUE))

# top words that appear at least 100 times
words %>% 
  count(word) %>% 
  filter(n > 100) %>% 
  arrange(desc(n))

# define AFINN lexicon
afinn <- get_sentiments("afinn")

afinn_sentiments <- words %>% 
  inner_join(afinn, by = "word")

afinn_sentiments %>% 
  filter(value > 0) %>% 
  count

afinn_sentiments %>% 
  filter(value == 4) %>% 
  count
