library(tidyverse)
library(nanoparquet)
library(tidytext)
library(ggtext)
library(fs)

read_delim_cc <- function(file) {
  read_delim(file, col_types = "cccccdd")
}

url <- paste0("https://kolkostruva.bg/opendata_files/", Sys.Date() - 1, ".zip")
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
  filter(naseleno_masto == 87374) %>% 
  filter_out(str_detect(naimenovanie_na_produkta, "^Krina;")) %>% 
  reframe(cena_na_drebno = round(mean(cena_na_drebno, na.rm = T), 2),
          cena_v_promocia = round(mean(cena_v_promocia, na.rm = T), 2),
          .by = c(market, kod_na_produkta, 
                  kategoria, naimenovanie_na_produkta)) %>%
  mutate(cena_v_promocia = as.character(cena_v_promocia),
         cena_v_promocia = str_replace(cena_v_promocia, "NaN", ""),
         cena_v_promocia = parse_number(cena_v_promocia),
         naimenovanie_na_produkta = reorder_within(
           naimenovanie_na_produkta, cena_na_drebno, market),
         date = as.character(Sys.Date() - 1), .before = everything())

df_markets <- read_parquet("shiny/markets/df_markets_2026.parquet")
df_markets <- bind_rows(df_markets, markets)

glimpse(df_markets)
df_markets %>% count(date) %>% print(n = Inf)

write_parquet(df_markets, "shiny/markets/df_markets_2026.parquet")

df_markets %>% 
  filter(date == "2026-01-01", 
         str_detect(naimenovanie_na_produkta, 
         regex("^(?=.*ябълки)(?=.*кг).*$", ignore_case = T))) %>%
  ggplot(aes(cena_na_drebno, naimenovanie_na_produkta, fill = market)) +
  geom_col(show.legend = F) +
  geom_richtext(aes(label = glue::glue("{cena_na_drebno};  <span style='color:red'>{cena_v_promocia}</span>")), 
                position = position_dodge(width = 1), hjust = -0.01, size = 4.5, fill = NA, label.colour = NA) +
  scale_y_reordered() +
  scale_x_continuous(expand = expansion(mult = c(.05, .7))) +
  labs(x = "Цена (евро); <span style='color:red'>Промоция (евро)</span>", y = NULL) +
  theme(text = element_text(size = 14), axis.title.x = element_markdown()) +
  facet_wrap(vars(market), scales = "free_y")

df_markets %>%
  mutate(naimenovanie_na_produkta = str_remove(naimenovanie_na_produkta, "___.+$"),
         date = ymd(date)) %>% 
  filter(str_detect(naimenovanie_na_produkta,
                    regex("^(?=.*ябълки)(?=.*)(?=.*кг).*$", ignore_case = T))) %>%
  pivot_longer(6:7) %>% drop_na(value) %>%
  mutate(market = glue::glue(" <span style='color:blue'>**{market}**</span>")) %>% 
  unite(c(market, naimenovanie_na_produkta), col = "market_product", sep = ": ") %>%
  ggplot(aes(date, value, group = name, color = name)) +
  geom_point(size = 1) +
  geom_line(linewidth = 0.3, linetype = 2) +
  scale_x_date(date_breaks = "15 days", date_labels = "%b-%d") +
  scale_color_manual(values = c("black", "red"), 
                     labels = c("Цена на дребно", "Цена в промоция")) +
  theme(text = element_text(size = 14), legend.position = "top",
        strip.text = element_markdown()) +
  labs(y = "Цена (евро)", x = "Дата", color = "Легенда:") +
  facet_wrap(vars(market_product))

colors_percent <- c("TRUE" = "#00BFC4", "FALSE" = "#F8766D")
df_markets %>%
  mutate(naimenovanie_na_produkta = str_remove(naimenovanie_na_produkta, "___.+$")) %>% 
  #summarise(cena_na_drebno = mean(cena_na_drebno, na.rm = T), .by = c(market, date)) %>% 
  filter(date %in% c("2026-01-01", "2026-01-03"), !cena_na_drebno == 0) %>%
  summarise(
    price_change = (last(cena_na_drebno) - first(
      cena_na_drebno)) / first(cena_na_drebno) * 100,
    .by = c(market, naimenovanie_na_produkta)) %>%
  filter(price_change != 0) %>%
  mutate(naimenovanie_na_produkta = fct_reorder(
    naimenovanie_na_produkta, price_change), col = price_change > 0) %>% 
  ggplot(aes(price_change, naimenovanie_na_produkta, fill = col)) +
  geom_col(show.legend = F) +
  scale_fill_manual(values = colors_percent) +
  scale_x_continuous(expand = expansion(mult = c(0.01, 0.10))) +
  geom_text(aes(label = paste0(round(price_change, 1), "%")),
            position = position_dodge(width = 1), hjust = -0.1, size = 3.5) +
  labs(y = NULL, x = NULL) +
  theme(text = element_text(size = 16), 
        axis.text.x = element_blank(), 
        axis.ticks.x = element_blank()) +
  facet_wrap(vars(market), scales = "free_y")

df_markets %>% filter(kategoria %in% c(29)) %>% view
df_markets %>%
  filter(date %in% c("2026-01-01", "2026-01-03"), !cena_na_drebno == 0) %>%
  #filter(kategoria %in% c(25:28)) %>% 
  summarise(cena_na_drebno = mean(cena_na_drebno, na.rm = T), .by = c(market, date)) %>% 
  summarise(
    price_change = (last(cena_na_drebno) - first(
      cena_na_drebno)) / first(cena_na_drebno) * 100,
    .by = c(market)) %>%
  filter(price_change != 0) %>%
  mutate(market = fct_reorder(market, price_change), col = price_change > 0) %>% 
  ggplot(aes(price_change, market, fill = col)) +
  geom_col(show.legend = F) +
  scale_fill_manual(values = colors_percent) +
  scale_x_continuous(expand = expansion(mult = c(0.01, 0.10))) +
  geom_text(aes(label = paste0(round(price_change, 2), "%")),
            position = position_dodge(width = 1), hjust = -0.1, size = 5) +
  labs(y = NULL, x = NULL) +
  theme(text = element_text(size = 18), 
        axis.text.x = element_blank(), 
        axis.ticks.x = element_blank())

df_markets %>%
  filter(
    kategoria == 27) %>%
  summarise(cena_na_drebno = mean(cena_na_drebno, na.rm = T),
            cena_v_promocia = mean(as.numeric(cena_v_promocia), na.rm = T),
            .by = c(market, kategoria, date)) %>%
  # filter(cena_na_drebno != 0) %>%
  mutate(
    market = fct_relevel(market, "Кауфланд", "Лидл", "Билла", "T Market", "Славекс", "Вилтон"),
    date = ymd(date)) %>%
  pivot_longer(4:5) %>% drop_na(value) %>%
  ggplot(aes(date, value, group = name, color = name)) +
  geom_point() +
  geom_line(linewidth = 0.3, linetype = 2) +
  scale_color_manual(values = c("black", "red"), 
                     labels = c("Цена на дребно", "Цена в промоция")) +
  scale_x_date(date_breaks = "20 days", date_labels = "%b-%d") +
  labs(y = "Средна цена (евро)", x = "Дата", color = "Легенда:") +
  theme(text = element_text(size = 14), legend.position = "top") +
  facet_wrap(vars(market), nrow = 1)
