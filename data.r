# ==============================================================================.
# 1. CONFIGURAÇÕES INICIAIS E BIBLIOTECAS -------
# ==============================================================================.

rm(list = ls()); gc()
# abre pacotes-------------------------------------------------------------

library(dplyr)
library(data.table)
library(reticulate)
library(sparklyr)
library(stringr)
library(lubridate)
library(DBI)
library(sparklyr)
library(stringr)
library(reticulate)
library(janitor)
library(readxl)
library(geobr)


# objetos e diretório  --------------------------------------------------------
dir.create("data",showWarnings = F)


# ==============================================================================.
# 2. CONEXÃO as fontes de dados -------
# ==============================================================================.


### 2.1 - conexão com extração do DW (Luciana) ----
path_dados <- "C:/Users/wesley.jesus/Documents/dados_publicacoes_tematicas"

df_serv <- fread(file.path(path_dados,"TODOS2.csv")) %>%
  janitor::clean_names()

df_cotas <- fread(file.path(path_dados,"COTAS_ATIVOS.csv")) %>%
  janitor::clean_names()

df_cotas_ind <- fread(file.path(path_dados,"COTAS_COMINDICADOR.csv")) %>%
  janitor::clean_names()

dic_cotas <- data.table(co_tipo_cota = 0:4,
                        no_tipo_cota =
                          c("Não informado",
                            "Não",
                            "Cota Racial",
                            "Cota PCD",
                            "Cota Indígena"))
# formatando datas
df_cotas[,anomes := as.Date(dt_lotacao_serv,"%d/%m/%Y") %>% format("%Y%m")]
df_cotas_ind[,`:=`(anomes = as.Date(dt_lotacao_serv,"%d/%m/%Y") %>% format("%Y%m"),
                   anomes_ex = as.Date(dt_ocor_exclusao_serv,"%d/%m/%Y") %>% format("%Y%m"))]


## aposentados e instituidores de penão
df_situacao_ind <- fread(file.path(path_dados,"pep_conceito_servidores_indigenas.txt")) %>%
  janitor::clean_names()


### TESTES (EXCLUIR DEPOIS)

# # checando onde tem data NA
# df_cotas[is.na(anomes),.N,.(dt_lotacao_serv)]
# df_cotas_ind[is.na(anomes),.N,.(dt_lotacao_serv)]
#
# # checando % de NA nas datas por marcador de cotas
# df_cotas[,.(.N,
#             n_data_na = sum(is.na(anomes)),
#             p_data_na = 100*mean(is.na(anomes)),
#             min_mes = min(anomes,na.rm = T),
#             max_mes = max(anomes,na.rm = T)),.(co_tipo_cota)]
#
# # checando filtros
# df_cotas[,.N,.(var_0048_qtd_serv_p,var_0001_situacao)]
# df_cotas_ind[,.N,.(var_0048_qtd_serv_p,var_0001_situacao)]


### faixa etária
idades <- df_serv[,.(idade)] %>% unique
idades[,faixa_etaria :=
         cut(idade,
             breaks = c(15,18,seq(25,65,5),125),
             right = T,
             include.lowest = T
             )
       ]

idades[,nome_faixa_etaria :=
         ifelse(is.na(faixa_etaria),
                "S/info",
                ifelse(idade > 65,
                       "Acima de 65 anos",
                       ifelse(idade < 19,
                              "15 a 18 anos",
                              paste0(min(idade)," a ",max(idade)," anos")
                              )
                       )
                ),
       .(faixa_etaria)
       ]

df_serv <- left_join(df_serv,idades)

### 2.2 - conexão com DataBricks unique()### 2.2 - conexão com DataBricks ----
use_virtualenv(Sys.getenv("venv_path"), required = TRUE)
sc <- spark_connect(
  master     = Sys.getenv("master"),
  method     = Sys.getenv("method"),
  cluster_id = Sys.getenv("cluster_id"),
  token      = Sys.getenv("token_databricks"),
  envname    = Sys.getenv("venv_path")
)

# Leitura dos dados apontando para o Volume Bronze
df_spark <- spark_read_parquet(
  sc,
  name = "servidores",
  path = "/Volumes/mgi-bronze/raw_data_volumes/mgi/cginf/servidores_dwsiape_mensal_funcoes/"
)



# ==============================================================================.
# 3. DATA WRANGLING (Spark + R) -------
# ==============================================================================.
df_etnia <- df_serv %>%
  group_by(mes, nome_cor_origem_etnica) %>%
  summarise(linhas = n(),
            n = sum(qtd),
            .groups = "drop") %>%
  collect() %>%
  mutate(
    # Limpeza de strings para garantir o match com a base
    nome_limpo = str_trim(nome_cor_origem_etnica),
    nome_limpo = str_to_upper(nome_limpo),

    # Identifica categoria indígena (robusto para variações)
    is_indigena = ifelse(str_detect(nome_limpo, "INDIGENA|INDÍGENA"), "Sim", "Não"),

    # Conversão de competência para formato de data
    data = ymd(paste0(mes, "01"))
  ) %>%
  group_by(data) %>%
  mutate(pct = n / sum(n)) %>%
  ungroup()


## salvando ---------
saveRDS(df_etnia,"data/df_etnia.rds")





# ==============================================================================.
# 4. Tabela por UF -------
# ==============================================================================.


# Filtrando apenas indígenas no último mês disponível (Fev 2026)
# O processamento acontece no cluster, não no seu PC!
df_indigenas_uf <- df_serv %>%
  filter(mes == 202602) %>%
  group_by(nome_cor_origem_etnica,uf) %>%
  summarise(total_indigenas = sum(qtd)) %>%
  collect() # Agora sim, trazemos apenas 27 linhas para o R


# Carregamento da malha via geobr
mapa_br <- geobr::read_state(year = 2020, showProgress = FALSE)

# Join Final
df_mapa_final <- mapa_br %>%
  left_join(df_indigenas_uf, by = c("abbrev_state" = "uf")) %>%
  mutate(total_indigenas = coalesce(total_indigenas, 0))

setorder(df_mapa_final,-total_indigenas)


## salvando
saveRDS(df_indigenas_uf,'data/df_indigenas_uf.rds')
saveRDS(df_mapa_final,'data/df_mapa_final.rds')


# ==============================================================================.
# 5. piramide ------
# ==============================================================================.

# 1. Agregação no Spark
df_piramide <-
  df_serv[
    # Filtro para o mês mais recente
    mes == 202602,
    .(total = sum(qtd)),
    .(nome_faixa_etaria,
      faixa_etaria,
      nome_sexo)
    ] %>%
  # Ajuste para criar os dois lados da pirâmide
  .[,total_ajustado := ifelse(nome_sexo == "Fem", -total, total)]


df_piramide_indigena <- df_serv %>%
  # Filtro para o mês mais recente e para indígenas
  filter(mes == 202602 & nome_cor_origem_etnica == "INDIGENA") %>%
  group_by(nome_faixa_etaria, faixa_etaria, nome_sexo) %>%
  summarise(total = sum(qtd), .groups = "drop") %>%
  collect() %>%
  mutate(
    # Ajuste para criar os dois lados da pirâmide
    total_ajustado = ifelse(nome_sexo == "Fem", -total, total)
  )

## salvando
saveRDS(df_piramide,'data/df_piramide.rds')
saveRDS(df_piramide_indigena,'data/df_piramide_indigena.rds')

# ==============================================================================.
# 6. dados Treemap ----
# ==============================================================================.


df_escol <- df_serv %>%
  filter(mes == 202602) %>%
  group_by(escolaridade,nome_cor_origem_etnica,nome_escolaridade) %>%
  summarise(total = sum(qtd), .groups = "drop") %>%
  setDT

df_escol[,nome_escolaridade := iconv(nome_escolaridade,"latin1","utf-8")]

de_para_escol <- readxl::read_excel('data/escolaridades.xlsx') %>%
  select(escolaridade,nome_escolaridade_completa,ordem)


df_treemap_ind <-
  # filter(nome_cor_origem_etnica == "INDIGENA")
  df_escol %>%
  left_join(de_para_escol) %>%
  group_by(nome_escolaridade_completa,nome_cor_origem_etnica,ordem) %>%
  summarise(total = sum(total), .groups = "drop") %>%
  setorder(ordem)

## reordenando
df_treemap_ind <-
  df_treemap_ind %>%
  mutate(nome_escolaridade.f = factor(nome_escolaridade_completa,
                                      levels = unique(nome_escolaridade_completa),
                                      ordered = T)) %>%
  setDT


saveRDS(df_treemap_ind,'data/df_treemap_ind.rds')


# ==============================================================================.
# 7. Por órgão e tipo de órgão ------
# ==============================================================================.

grupos_natjur <-
  read_excel('data/tb_auxiliares.xlsx') %>%
  setDT %>%
  select(cod_orgao,no_orgao,sg_orgao,agrup_orgao2,agrup_orgao1)


# total por nome_natureza_juridica_cnnj
df_natjur <- df_serv %>%
  filter( grepl("IND.GENA",nome_cor_origem_etnica,ignore.case = T)) %>%
  group_by(ano,mes,nome_cor_origem_etnica,nome_natureza_juridica_cnnj) %>%
  summarise(total = sum(qtd))

# total por órgão no último mês
df_orgao <- df_serv %>%
  filter(mes == 202602) %>%
  group_by(ano,orgao_vinc,nome_orgao_vinc,no_orgao,nome_cor_origem_etnica) %>%
  summarise(total = sum(qtd))

saveRDS(df_natjur,'data/df_natjur.rds')
saveRDS(df_orgao,'data/df_orgao.rds')


# ==============================================================================.
# 8. Por etnia, efetivos, função, etc ------
# ==============================================================================.

## total de efetivos
df_efetivos <-
  df_serv %>%
  group_by(ano,
           mes,
           nome_cor_origem_etnica,
           nome_sexo,
           nome_cargo_origem,
           nome_cargo,
           nome_faixa_etaria) %>%
  summarise(total = sum(qtd)) %>%
  collect %>%
  mutate(efetivo = !(grepl("s/(cargo|info)",nome_cargo,ignore.case = T) &
                       grepl("s/(cargo|info)",nome_cargo_origem,ignore.case = T)
                     )
         ) %>% #View
  group_by(mes,nome_cor_origem_etnica,
           nome_sexo,nome_faixa_etaria,
           efetivo) %>%
  summarise(total = sum(total)) %>%
  setDT


## total por função e etnia
df_funcao <-
  df_spark %>%
  group_by(mes,
           nome_cor_origem_etnica,
           nome_sexo,
           nome_funcao,
           nome_nivel_funcao) %>%
  summarise(total = sum(qtde_vinculos)) %>%
  collect %>%
  setDT


# mexendo nos níveis
df_funcao <-
  df_funcao %>%
  # filter(mes == 202602 & nome_funcao %in% c("CCX","FEX")) %>% #View
  mutate(
    decreto_nivel = gsub("(CCX|FEX)-[0-9]{2}","",nome_nivel_funcao) %>%
      as.numeric
  ) %>%
  mutate(intervalo_nivel = cut(decreto_nivel,
                               breaks = c(1,5,7,10,13,15,19),
                               include.lowest= T,
                               right= F,
                               ordered_result = T
                               )) %>%
  setDT %>% #View
  .[,`:=`(label_intervalo =
            paste0(
              min(decreto_nivel,na.rm = T),
              ifelse(n_distinct(decreto_nivel,na.rm = T) == 1,
                     "",
                     ifelse(n_distinct(decreto_nivel,na.rm = T) == 2,
                            " e ",
                            " a ") %>% paste0(.,
                                              max(decreto_nivel,na.rm = T)
                                              )
                     )
              ) %>%
            paste0("Nível ",.)
          ),
    .(intervalo_nivel)]

df_funcao[is.na(intervalo_nivel),
          `:=`(label_intervalo = "S/função FCE/CCE")]

ord_levels <- df_funcao[,.(intervalo_nivel,label_intervalo)] %>%
  unique %>%
  setorder(intervalo_nivel) %>%
  .$label_intervalo %>%
  as.character()

## juntando níveis e totais
df_funcao_total <-
  df_efetivos[,.(total = sum(total)),
              .(mes,
                etnia = ifelse(nome_cor_origem_etnica == "INDIGENA",
                               "Indígenas",
                               "Demais Raça/cor"),
                nome_sexo)] %>%
  left_join(
    df_funcao[!is.na(intervalo_nivel),
              .(total_funcao = sum(total)),
              .(mes,
                etnia = ifelse(nome_cor_origem_etnica == "INDIGENA",
                               "Indígenas",
                               "Demais Raça/cor"),
                nome_sexo,
                label_intervalo
                # intervalo_nivel
                )] %>%
      dcast(mes + etnia + nome_sexo ~ label_intervalo,value = "total_funcao",fill = 0)
  ) %>%
  melt(id.vars = c("mes","etnia","nome_sexo","total"),
       variable.name = "nivel",
       value.name = "total_funcao",
       na.rm = F) %>%
  .[,`:=`(nivel_sexo = factor(nome_sexo,levels = c('Mas','Fem'),ordered = T),
          nivel_ord =  factor(nivel,levels = ord_levels,ordered = T),
          total_funcao = ifelse(is.na(total_funcao),0,total_funcao))] %>%
  .[,total_funcao_adj := ifelse(nome_sexo == "Fem",-total_funcao,total_funcao)] %>%
  .[,p_sexo := round(100*total_funcao_adj/sum(total_funcao),1),
    .(mes,etnia,nivel)]





## juntando efetivos
df_funcao_efetivos <-
  df_efetivos[(efetivo),.(total = sum(total)),
              .(mes,
                etnia = ifelse(nome_cor_origem_etnica == "INDIGENA",
                               "Indígenas",
                               "Demais Raça/cor"),
                nome_sexo)] %>%
  .[,p_total := 100*total/sum(total),
    .(mes,nome_sexo)] %>%
  left_join(
    df_funcao[nome_funcao %in% "FEX",
              .(total_funcao = sum(total)),
              .(mes,
                etnia = ifelse(nome_cor_origem_etnica == "INDIGENA",
                               "Indígenas",
                               "Demais Raça/cor"),
                nome_sexo,
                label_intervalo
                # intervalo_nivel
              )] %>%
      dcast(mes + etnia + nome_sexo ~ label_intervalo,value = "total_funcao",fill = 0)
  ) %>%
  melt(id.vars = c("mes","etnia","nome_sexo","total","p_total"),
       variable.name = "nivel",
       value.name = "total_funcao",
       na.rm = F) %>%
  .[,`:=`(nivel_sexo = factor(nome_sexo,levels = c('Mas','Fem'),ordered = T),
          nivel_ord =  factor(nivel,levels = ord_levels,ordered = T),
          total_funcao = ifelse(is.na(total_funcao),0,total_funcao))] %>%
  .[,total_funcao_adj := ifelse(nome_sexo == "Fem",-total_funcao,total_funcao)] %>%
  .[,p_sexo := round(100*total_funcao_adj/sum(total_funcao),1),
    .(mes,etnia,nivel)] %>%
  .[,p_etnia := 100*total_funcao/sum(total_funcao),.(mes,nome_sexo,nivel)] %>%
  filter(etnia == "Indígenas")



saveRDS(df_efetivos,'data/df_efetivos.rds')
saveRDS(df_funcao,'data/df_funcao.rds')
saveRDS(df_funcao_total,'data/df_funcao_total.rds')
saveRDS(df_funcao_efetivos,"data/df_funcao_efetivos.rds")

# ==============================================================================.
# 9. Ingressos por cotas ------
# ==============================================================================.

df_cotas[,
         .(total = .N),
         .(mes = anomes,
           no_cor_origem_etnica,
           co_tipo_cota)
         ] -> serie_ingressos

df_cotas_ind[,
             .(total = .N),
             .(mes = anomes,
               no_cor_origem_etnica,
               co_tipo_cotaefetivo = !(grepl("s/(cargo|info)",no_cargo,ignore.case = T) &
                                         grepl("s/(cargo|info)",no_cargo_origem,ignore.case = T)
               ))
             ] -> serie_cotas

saveRDS(serie_cotas,'data/serie_cotas.rds')



# full_join(
#   df_cotas_ind[no_cor_origem_etnica == "INDIGENA",
#                .(total_cotistas = .N),
#                .(ano_ingresso = substr(anomes,1,4) %>% as.numeric(),
#                  co_tipo_cota)
#                ],
#
#   df_cotas[no_cor_origem_etnica == "INDIGENA",
#            .(total_ativos_hoje = .N),
#            .(ano_ingresso =  substr(anomes,1,4) %>% as.numeric(),
#              co_tipo_cota)
#            ]
#
#   ) %>% View
#
#
#
# df_cotas[no_cor_origem_etnica == "INDIGENA" &
#            !is.na(anomes),
#          .(.N),
#          .(co_tipo_cota)] %>%
#   .[,p := 100*N/sum(N)] %>% View
#
#
# df_cotas[no_cor_origem_etnica == "INDIGENA" &
#            !is.na(anomes),
#          .(.N,
#            ),
#          .(anomes,
#            ativo =  ] %>%
#   filter(ativo) %>% View


# ==============================================================================.
# 10. Saídas por aposentadoria ------
# ==============================================================================.

df_indigenas_sit <- df_situacao_ind[,.(total = sum(v16)),
                                    .(situacao,
                                      efetivo = !(grepl("s/(cargo|info)",cargo,ignore.case = T) &
                                                    grepl("s/(cargo|info)",cargo_origem,ignore.case = T)
                                                  ),
                                      mes.f =
                                        mes %>%
                                        paste0("01 ",.) %>%
                                        tolower %>%
                                        as.Date("%d %b %Y") %>%
                                        format("%Y%m")
                                      )] %>%
  setorder(situacao,mes.f)


# df_indigenas_sit  %>%
#   filter(!is.na(mes.f)) %>%
#   ggplot_cat(aes(x = as.numeric(substr(mes.f,1,4)),
#                  y = total,
#                  col =
#                    paste0(
#                      ifelse(
#                        efetivo,
#                        "Efetivo ",
#                        "Não efetivo "
#                        ),
#                      situacao)
#                  )
#              ) +
#   geom_line(size = 2) +
#   geom_point(size = 3)


saveRDS(df_indigenas_sit,"data/df_indigenas_sit")

# ==============================================================================.
# 10. fechando conexão -----
# ==============================================================================.

spark_disconnect(sc)

