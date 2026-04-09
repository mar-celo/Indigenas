# ==============================================================================
# 1. CONFIGURAÇÕES INICIAIS E BIBLIOTECAS -------
# ==============================================================================


# abre pacotes-------------------------------------------------------------

library(dplyr)
library(reticulate)
library(sparklyr)
library(stringr)
library(lubridate)
library(DBI)
library(sparklyr)
library(stringr)
library(reticulate)

# pacotes <- c('arrow',
#              'dplyr',
#              'lubridate',
#              'DBI',
#              'sparklyr',
#              'stringr',
#              'reticulate'
#              )
#
# for(i in pacotes){
#   if(!require(i, character.only = TRUE )){
#     install.packages(i, dependencies = TRUE)
#     require(i, character.only = TRUE )
#   }
# }




# objetos e diretório  --------------------------------------------------------
dir.create("data",showWarnings = F)


# ==============================================================================
# 2. CONEXÃO COM O DATABRICKS -------
# ==============================================================================



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
df_etnia <- df_spark %>%
  group_by(mes, nome_cor_origem_etnica) %>%
  summarise(linhas = n(),
            n = sum(qtde_vinculos),
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
df_indigenas_uf <- df_spark %>%
  filter(mes == 202602) %>%
  group_by(nome_cor_origem_etnica,uf) %>%
  summarise(total_indigenas = sum(qtde_vinculos)) %>%
  collect() # Agora sim, trazemos apenas 27 linhas para o R


## salvando
saveRDS(df_indigenas_uf,'data/df_indigenas_uf.rds')


# ==============================================================================
# 6. piramide ------
# ==============================================================================

# 1. Agregação no Spark
df_piramide <- df_spark %>%
  filter(mes == 202602) %>% # Filtro para o mês mais recente
  group_by(nome_faixa_etaria, faixa_etaria, nome_sexo) %>%
  summarise(total = sum(qtde_vinculos), .groups = "drop") %>%
  collect() %>%
  mutate(
    # Ajuste para criar os dois lados da pirâmide
    total_ajustado = ifelse(nome_sexo == "Fem", -total, total)
  )


df_piramide_indigena <- df_spark %>%
  # Filtro para o mês mais recente e para indígenas
  filter(mes == 202602 & nome_cor_origem_etnica == "INDIGENA") %>%
  group_by(nome_faixa_etaria, faixa_etaria, nome_sexo) %>%
  summarise(total = sum(qtde_vinculos), .groups = "drop") %>%
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


reorg_escol <-
  c("ANALFABETO" =  "NÃO ALFABETIZADO",
    "ENSINO FUNDAMENTAL INCOMPLETO"  = "ALFABETIZADO",
    "ALFABETIZADO SEM CURSOS REGULA"  = "ALFABETIZADO",
    "PRIMEIRO GRAU INCOMP.-ATE A 4A"  = "ALFABETIZADO",
    "4A. SERIE DO PRIMEIRO GRAU COM"  = "ALFABETIZADO",
    "ENSINO FUNDAMENTAL"  = "ENSINO FUNDAMENTAL",
    "SEGUNDO GRAU INCOMPLETO"  = "ENSINO FUNDAMENTAL",
    "SUPERIOR INCOMPLETO" = "ENSINO MÉDIO",
    "ENSINO MEDIO" = "ENSINO MÉDIO",
    "ENSINO SUPERIOR" = "ENSINO SUPERIOR",
    "MESTRADO" = "MESTRADO",
    "DOUTORADO" = "DOUTORADO"
  )

reorg_escol2 <-
  c("Sem instrução" = "Não alfabetizado",
    "Ensino Fundamental Incompleto" = "Alfabetizado",
    "Ensino Fundamental Completo" = "Ensino Fundamental",
    "Ensino Médio Incompleto" = "Ensino Fundamental",
    "Ensino Médio Completo" = "Ensino Médio",
    "Ensino Superior Incompleto" = "Ensino Médio",
    "Ensino Superior Completo" = "Ensinio Superior",
    "Pós-Graduação" = "Pós-Graduação")


df_treemap_ind <- df_spark %>%
  filter(mes == 202602, nome_cor_origem_etnica == "INDIGENA") %>%
  group_by(nome_escolaridade_completa) %>%
  summarise(total = sum(qtde_vinculos), .groups = "drop") %>%
  collect() %>%
  mutate(
    nome_escolaridade_new = reorg_escol2[str_trim(nome_escolaridade_completa)]
  ) %>%
  group_by(nome_escolaridade_new) %>%
  summarise(total = sum(total), .groups = "drop")

## reordenando
df_treemap_ind <-
  df_treemap_ind %>%
  mutate(nome_escolaridade.f = factor(nome_escolaridade_new,
                                      levels = unique(reorg_escol2),
                                      ordered = T))


saveRDS(df_treemap_ind,'data/df_treemap_ind.rds')



# ==============================================================================
# 8. fechando conexão
# ==============================================================================

spark_disconnect(sc)
