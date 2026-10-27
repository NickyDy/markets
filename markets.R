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
         date = as.character(Sys.Date() - 1), .before = everything()) %>% 
  mutate(kategoria_c = fct_collapse(kategoria, 
"Бял хляб" = "1", "Кашкавал от краве мляко" = c("10", "11"),
"Краве масло" = "12", "Извара" = c("13", "14"), "Цяло пиле" = "15",
"Пилешко филе" = "16", "Пилешко бутче" = "17", "Свинска плешка" = "18",
"Свински бут" = "19", "Хляб Добруджа" = "2", "Свинско филе" = "20",
"Свински врат" = "21", "Свинско месо за готвене" = "22",
"Кайма смес" = "25", "Кренвирши" = "26", "Колбаси" = c("24", "27"),
"Салами" = "28", "Замразена риба" = "29", "Ръжен хляб" = "3",
"Охладена риба" = "30", "Яйца M" = "31", "Яйца L" = "32",
"Боб" = "33", "Леща" = "34", "Ориз" = "35", "Макарони" = "36",
"Спагети" = "37", "Захар" = "38", "Сол" = "39", "Типов хляб" = "4",
"Брашно тип 500" = "40", "Брашно екстра" = "41", "Олио" = "42",
"Маслиново масло" = "43", "Винен оцет" = "44", "Ябълков оцет" = "45",
"Бял боб (консерва)" = "46", "Зелен грах (консерва)" = "47",
"Домати (консерва)" = "48", "Лютеница" = "49", "Точени кори" = "5",
"Лимони" = "50", "Портокали" = "51", "Банани" = "52", "Ябълки" = "53",
"Домати" = "54", "Кромид лук" = "55", "Моркови" = "56", "Зеле" = "57",
"Краставици" = "58", "Чесън" = "59", "Прясно мляко" = "6",
"Гъби" = "60", "Картофи" = "61", "Маслини" = "62", "Каша" = "63",
"Пюре" = "64", "Сухо мляко" = "65", "Бисквити" = "66",
"Кроасани" = "67", "Банички" = "68", "Шоколади" = "69",
"Кисело мляко" = "7", "Мляно кафе" = "70", "Кафе на зърна" = "71",
"Чай" = "72", "Минерална вода" = "73", "Бира" = "74", "Бяло вино" = "75",
"Червено вино" = "76", "Ракия" = "77", "Цигари" = "78",
"Препарат за съдове" = "79", "Краве сирене" = c("8", "9"), "Четки за зъби" = "80",
"Паста за зъби" = "81", "Шампоани" = "82", "Сапуни" = "83",
"Мокри кърпички" = "84", "Тоалетна хартия" = "85"), .after = kategoria) %>%
  mutate(kategoria = as.numeric(kategoria), 
         naimenovanie_na_produkta = as.character(naimenovanie_na_produkta)) %>% 
  filter(!cena_na_drebno == 0, 
         !str_detect(kategoria, "0043157"),
         !str_detect(kategoria, "0138003"),
         !str_detect(kategoria, "5568852"),
         !str_detect(kategoria, "6800057"),
         !str_detect(naimenovanie_na_produkta, "^Krina"),
         !str_detect(naimenovanie_na_produkta, "Гръцко краве сирене"), 
         !str_detect(naimenovanie_na_produkta, "Сал.сирене от кр.мляко с пов.вод.съ"),
         !str_detect(naimenovanie_na_produkta, "Обикновени бисквити"),
         !str_detect(naimenovanie_na_produkta, "КРАВЕ МАСЛО D.MARKENBUTTER"),
         !str_detect(naimenovanie_na_produkta, "NN DeutscheMarkenbutter Кр масло 82%250г"))

df_markets <- read_parquet("shiny/markets/df_markets.parquet")

df_markets <- bind_rows(df_markets, markets)

df_markets %>% count(date) %>% print(n = Inf)

write_parquet(df_markets, "shiny/markets/df_markets.parquet")

glimpse(df_markets)

df_markets %>%
  mutate(naimenovanie_na_produkta = str_remove(naimenovanie_na_produkta, "___.+$")) %>%
  filter(date == "2026-10-22", !cena_na_drebno == 0, kategoria %in% c(1:30),
         !naimenovanie_na_produkta %in% c("ДЕЛИКАТЕС С ПУЕШКО ФИЛЕ МАЙСТОР ЦВЕТКО")
  ) %>%
  summarise(group_price = mean(cena_na_drebno, na.rm = T), .by = c(market, kategoria_c)) %>%
  mutate(
    market = fct_relevel(market, "Кауфланд", "Лидл", "Билла", "T Market", "Славекс", "Вилтон"),
    ) %>% 
  ggplot(aes(group_price, reorder_within(market, group_price, kategoria_c), fill = market)) +
  geom_col(show.legend = F) +
  scale_fill_manual(values = c("Кауфланд" = "red", "Лидл" = "yellow", "T Market" = "green",
                               "Билла" = "pink", "Вилтон" = "blue", "Славекс" = "purple")) +
  scale_x_continuous(expand = expansion(mult = c(0.01, 0.5))) +
  scale_y_reordered() +
  geom_text(aes(label = paste0(round(group_price, 1), " €")),
            position = position_dodge(width = 1), hjust = -0.1, size = 3.5) +
  labs(y = NULL, x = NULL, caption = 'Източник на данните: "КОЛКО СТРУВА"',
       title = paste0("Средна цена по групи продукти:")) +
  theme(text = element_text(size = 14), axis.text.x = element_blank(), axis.ticks.x = element_blank(),
        strip.text = element_text(face = "bold")) +
  facet_wrap(vars(kategoria_c), scales = "free_y")
