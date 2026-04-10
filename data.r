# ==============================================================================
# 1. CONFIGURAÇÕES INICIAIS E BIBLIOTECAS -------
# ==============================================================================


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


# objetos e diretório  --------------------------------------------------------
dir.create("data",showWarnings = F)


# ==============================================================================
# 2. CONEXÃO as fontes de dados -------
# ==============================================================================


### 2.1 - conexão com extração do DW (Luciana) ----
path_dados <- "C:/Users/wesley.jesus/Documents/dados_publicacoes_tematicas"

df_serv <- fread(file.path(path_dados,"TODOS2.csv")) %>%
  janitor::clean_names()


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



# ==============================================================================
# 3. DATA WRANGLING (Spark + R) -------
# ==============================================================================
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





# ==============================================================================
# 5. Tabela por UF -------
# ==============================================================================


# Filtrando apenas indígenas no último mês disponível (Fev 2026)
# O processamento acontece no cluster, não no seu PC!
df_indigenas_uf <- df_serv %>%
  filter(mes == 202602) %>%
  group_by(nome_cor_origem_etnica,uf) %>%
  summarise(total_indigenas = sum(qtd)) %>%
  collect() # Agora sim, trazemos apenas 27 linhas para o R


## salvando
saveRDS(df_indigenas_uf,'data/df_indigenas_uf.rds')


# ==============================================================================
# 6. piramide ------
# ==============================================================================

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

# ==============================================================================
# 7. dados Treemap
# ==============================================================================


df_escol <- df_serv %>%
  filter(mes == 202602, nome_cor_origem_etnica == "INDIGENA") %>%
  group_by(escolaridade,nome_escolaridade) %>%
  summarise(total = sum(qtd), .groups = "drop") %>%
  setDT

df_escol[,nome_escolaridade := iconv(nome_escolaridade,"latin1","utf-8")]

de_para_escol <- readxl::read_excel('data/escolaridades.xlsx') %>%
  select(escolaridade,nome_escolaridade_completa,ordem)


df_treemap_ind <-
  df_escol %>%
  left_join(de_para_escol) %>%
  group_by(nome_escolaridade_completa,ordem) %>%
  summarise(total = sum(total), .groups = "drop") %>%
  setorder(ordem)

## reordenando
df_treemap_ind <-
  df_treemap_ind %>%
  mutate(nome_escolaridade.f = factor(nome_escolaridade_completa,
                                      levels = nome_escolaridade_completa,
                                      ordered = T))


saveRDS(df_treemap_ind,'data/df_treemap_ind.rds')


# ==============================================================================
# 8. Por órgão e tipo de órgão ------
# ==============================================================================

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

# ==============================================================================
# 9. fechando conexão
# ==============================================================================

spark_disconnect(sc)

