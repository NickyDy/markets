library(tidyverse)
library(nanoparquet)
library(tidytext)
library(ggtext)
library(fs)

read_delim_cc <- function(file) {
  read_delim(file, col_types = "cccccdd")
}

url <- "https://kolkostruva.bg/opendata_files/2025-11-23.zip"
destfile <- tempfile(pattern = 'markets', tmpdir = tempdir(), fileext = '.zip')
download.file(url, destfile, quiet = T)
zip <- dir_ls(tempdir(), glob = "*.zip")
unzip(zip, exdir = tempdir())
files <- dir_ls(tempdir(), regexp = "Кауфланд |Лидл |T Market |Билла |Вилтон |Славекс ")

markets <- map(files, read_delim_cc) %>%
  set_names(basename) %>%
  list_rbind(names_to = "market") %>%
  bind_rows() %>% distinct() %>% janitor::clean_names() %>%
  mutate(market = str_extract(market, "Кауфланд|Лидл|T Market|Билла|Вилтон|Славекс")) %>% 
  mutate(cena_v_promocia = na_if(cena_v_promocia, 0)) %>% 
  filter(naseleno_masto == 87374,
         !str_detect(naimenovanie_na_produkta, "^Krina;")) %>% 
  reframe(cena_na_drebno = round(mean(cena_na_drebno, na.rm = T), 2),
          cena_v_promocia = round(mean(cena_v_promocia, na.rm = T), 2),
          .by = c(market, kod_na_produkta, 
                  kategoria, naimenovanie_na_produkta)) %>%
  mutate(cena_v_promocia = as.character(cena_v_promocia),
         cena_v_promocia = str_replace(cena_v_promocia, "NaN", ""),
         naimenovanie_na_produkta = reorder_within(naimenovanie_na_produkta, cena_na_drebno, market),
         date = "2025-11-23", .before = everything())

df_markets <- read_parquet("markets/df_markets.parquet")
df_markets <- bind_rows(df_markets, markets)

write_parquet(df_markets, "markets/df_markets.parquet")

glimpse(df_markets)
df_markets %>% count(date)

df_markets %>% 
  filter(date == "2025-11-23", str_detect(naimenovanie_na_produkta, 
                                          regex("^(?=.*ябълки)(?=.*кг).*$", ignore_case = T))) %>%
  ggplot(aes(cena_na_drebno, naimenovanie_na_produkta, fill = market)) +
  geom_col(show.legend = F) +
  geom_richtext(aes(label = glue::glue("{cena_na_drebno};  <span style='color:red'>{cena_v_promocia}</span>")), 
                position = position_dodge(width = 1), hjust = -0.01, size = 4.5, fill = NA, label.colour = NA) +
  scale_y_reordered() +
  scale_x_continuous(expand = expansion(mult = c(.05, .7))) +
  labs(x = "Цена (лв); <span style='color:red'>Промоция (лв)</span>", y = NULL) +
  theme(text = element_text(size = 14), axis.title.x = element_markdown()) +
  facet_wrap(vars(market), scales = "free_y")

df_markets %>% 
  mutate(naimenovanie_na_produkta = str_remove(naimenovanie_na_produkta, "___.+$"),
         date = ymd(date), cena_v_promocia = parse_number(cena_v_promocia)) %>% 
  filter(str_detect(naimenovanie_na_produkta, 
                    regex("^(?=.*ябълки)(?=.*)(?=.*кг).*$", ignore_case = T))) %>%
  pivot_longer(6:7) %>% drop_na(value) %>%
  mutate(market = glue::glue(" <span style='color:blue'>**{market}**</span>"),) %>% 
  unite(c(market, naimenovanie_na_produkta), col = "market_product", sep = ": ") %>%
  ggplot(aes(date, value, group = name, color = name)) +
  geom_point() +
  geom_line(linewidth = 0.3, linetype = 2) +
  scale_x_date(date_breaks = "3 days", date_labels = "%b-%d") +
  scale_color_manual(values = c("black", "red"), 
                     labels = c("Цена на дребно", "Цена в промоция")) +
  theme(text = element_text(size = 14), legend.position = "top",
        strip.text = element_markdown()) +
  labs(y = "Цена (лв)", x = "Дата", color = "Легенда:") +
  facet_wrap(vars(market_product), ncol = 4)

# billa <- read_csv("https://kolkostruva.bg/upload/11978/import.csv", col_types = c("cccccdd")) %>% 
#   mutate(market = "Била", .before = everything())
# vilton <- read_csv("https://kolkostruva.bg/upload/11933/import.csv", col_types = c("cccccdd")) %>% 
#   mutate(market = "Вилтон", .before = everything())
# kaufland <- read_csv("https://kolkostruva.bg/upload/11988/import.csv", col_types = c("cccccdd")) %>% 
#   mutate(market = "Кауфланд", .before = everything())
# lidl <- read_csv("https://kolkostruva.bg/upload/11943/import.csv", col_types = c("cccccdd")) %>% 
#   mutate(market = "Лидл", .before = everything())
# slaveks <- read_csv("https://kolkostruva.bg/upload/11561/PriceExportKzp_2025_11_03.csv", col_types = c("cccccdd")) %>% 
#   mutate(market = "Славекс", .before = everything())
# tmarket <- read_csv("https://kolkostruva.bg/upload/11583/KZP_03.11.2025.csv", col_types = c("cccccdd")) %>% 
#   mutate(market = "Т Маркет", .before = everything())

# markets <- bind_rows(billa, vilton, kaufland, lidl, slaveks, tmarket) %>% janitor::clean_names() %>% 
#   mutate(cena_v_promocia = na_if(cena_v_promocia, 0))