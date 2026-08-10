library(tidyverse)
library(tabulapdf)
library(nanoparquet)

link <- "https://www.dksbt.bg/doc/%D0%A1%D0%B5%D0%B4%D0%BC%D0%B8%D1%87%D0%B5%D0%BD%20%D0%B1%D1%8E%D0%BB%D0%B5%D1%82%D0%B8%D0%BD%20%2003%20%20-%2007%20%D0%B0%D0%B2%D0%B3%D1%83%D1%81%D1%82%20NEW%202026.pdf"

table <- extract_tables(link, col_names = F, method = "stream", pages = 1, output = "tibble") %>% pluck(1) %>% 
  drop_na() %>% 
  separate_wider_delim(cols = X5, names = c("X5", "X5_1"), delim = " ") %>%
  separate_wider_delim(cols = X7, names = c("X7", "X7_1"), delim = " ") %>%
  separate_wider_delim(cols = X8, names = c("X8", "X8_1", "x_8_2"), delim = " ") %>%
  separate_wider_delim(cols = X12, names = c("X12", "X12_1"), delim = " ") %>%
  separate_wider_delim(cols = X10, names = c("X10", "X10_1"), delim = " ") %>%
  select(product = X1, unit = X2, 
         "2026-08-03" = X4, 
         "2026-08-04" = X6, 
         "2026-08-05" = X8, 
         "2026-08-06" = X9, 
         "2026-08-07" = X11
         ) %>% 
  mutate(product = fct_recode(product, 'Брашно тип "500" /пакет 1 кг/' = "/пакет 1 кг/",
                              "Колбаси малотрайни /в т.ч. шунка/" = "малотрайни /в т.ч. шунка/",
                              "Олио /пластмасова бутилка1л/" = "/пластмасова бутилка1л/",
                              "Кисело мляко 3 и над 3% кофичка 400 г" = "кофичка 400 г",
                              "Прясно мляко 3% кутия/бутилка 1 л" = "кутия/бутилка 1 л")) %>% 
  pivot_longer(-c(product, unit)) %>%
  mutate(unit = str_remove(unit, ","), date = as.Date(name), value = str_replace(value, ",", "."),
         price = as.numeric(value)) %>% 
  select(product, unit, date, price)

df_market <- read_parquet("~/Desktop/R/shiny/markets/df_market.parquet")
df_market <- bind_rows(df_market, table)

write_parquet(df_market, "~/Desktop/R/shiny/markets/df_market.parquet")

table %>% count(product) %>% view

table %>% print(n = Inf)

extr_tables_page_1 <- function(links) {
  
  extract_tables(dams, col_names = F, method = "stream", pages = c(3:5))
  
}

df_p1 <- map(links1, extr_tables_page_1) %>% 
  map_dfr(., bind_rows) %>% 
  filter(str_detect(X1, "^\\d{2}\\.")) %>% select(-X16) %>% 
  select(date = X1, Домати = X2, Краставици = X3, Картофи = X4, Зеле = X5, Моркови = X6,
         `Лук зрял` = X7, `Червен пипер` = X8, `Зелен пипер` = X9, Тиквички = X10,
         Лимони = X11, Праскови = X12, Кайсии = X13, Дини = X14, Ябълки = X15) %>% 
  mutate(date = dmy(paste0(date, "2025"))) %>% 
  mutate(across(everything(), ~str_replace_all(., ",", "."))) %>% 
  distinct()


tab_1 <- tab[[1]] %>% 
  filter(str_detect(X1, "^\\d{2}\\.")) %>% select(-X16) %>% 
  select(date = X1, Домати = X2, Краставици = X3, Картофи = X4, Зеле = X5, Моркови = X6,
         `Лук зрял` = X7, `Червен пипер` = X8, `Зелен пипер` = X9, Тиквички = X10,
         Лимони = X11, Праскови = X12, Кайсии = X13, Дини = X14, Ябълки = X15) %>% 
  mutate(date = dmy(paste0(date, "2025"))) %>% 
  mutate(across(everything(), ~str_replace_all(., ",", ".")))

tab <- extract_tables(july_21_2025, col_names = F, method = "stream", pages = 3)

tab_2 <- tab[[1]] %>% 
  slice(6:15) %>% 
  select(Захар = X2, Фасул = X3, Ориз = X4, Брашно = X5, Олио = X6,
         Яйца = X7, Кашкавал = X8, Сирене = X9, `МАСЛО/125г` = X10,
         `Кисело мляко над 3%` = X11, `Прясно мляко 3%` = X12, `Свинско месо` = X13, Пиле = X14) %>% 
  mutate(across(everything(), ~str_replace_all(., ",", ".")))

july_22_2025 <- bind_cols(tab_1, tab_2)
july_21_2025 <- bind_cols(tab_1, tab_2)

july_2025 <- bind_rows(tab_1, tab_2) %>% distinct()
#----------------------------------------------------------------
june_2025 <- "https://www.dksbt.bg/doc/%20%D1%86%D0%B5%D0%BD%D0%B8%20%D1%8E%D0%BD%D0%B8%202025_DKSBT.pdf"
pages <- seq(from = 1, to = 101, by = 5)
table <- extract_tables(june_2025, col_names = F, method = "stream", pages = pages) %>% bind_rows()

tab <- table %>% 
  mutate(across(everything(), ~str_replace_all(., ",", "."))) %>% 
  filter(str_detect(X1, "^\\d{2}\\.")) %>% select(-X16) %>% 
  separate_wider_delim(c(X1), names = c("X1", "X1_1"), delim = " ", too_few = "align_start") %>%
  separate_wider_delim(c(X2), names = c("X2", "X2_1"), delim = " ", too_few = "align_start")

write_csv(tab, "tab.csv")
tab <- read_csv("tab.csv") %>% distinct()

tab %>% 
  select(date = X1, Домати = X2, Краставици = X3, Картофи = X4, Зеле = X5, Моркови = X6,
         `Лук зрял` = X7, `Червен пипер` = X8, `Зелен пипер` = X9, Тиквички = X10,
         Лимони = X11, Праскови = X12, Кайсии = X13, Дини = X14, Ябълки = X15) %>% 
  mutate(date = dmy(paste0(date, "2025")))

bak <- table[[3]] %>% 
  slice(6:15) %>% 
  select(Захар = X2, Фасул = X3, Ориз = X4, Брашно = X5, Олио = X6,
         Яйца = X7, Кашкавал = X8, Сирене = X9, `МАСЛО/125г` = X10,
         `Кисело мляко над 3%` = X11, `Прясно мляко 3%` = X12, `Свинско месо` = X13, Пиле = X14) %>% 
  mutate(across(everything(), ~str_replace_all(., ",", ".")))

