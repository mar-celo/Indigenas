library(rmarkdown)
library(ggplot2)


### opções de tamanhos das figuras
fw <- 15
fh <- 15

### opções de chunk
knitr::opts_chunk$set(list(warning = FALSE,
                           message = FALSE,
                           error = FALSE,
                           echo = FALSE,
                           fig.width = fw,
                           fig.height = fh,
                           dev = c('png','svg')
                           )
                      )



### opções de tamanhos de texto dos itens (eixo, legendas, etc)
txt_size <- 15

### opção de tamanho de texto no label (geom_text)
txt_geom_size <- 5

# paleta de cores para variáveis categóricas
# plt_dn1i_cat <- "Paired"
pal_primaria <- c('#0000AA',
                  ponto = '#FF7800',
                  principal = '#00A100')# COR PRINCIPAL


pal_complementar <- c(pal_primaria,
                      '#FFCE08',
                      '#EE3A79',
                      '#37C2D5',
                      '#C13C01',
                      '#9B83D9')


update_geom_defaults("col",list(fill = pal_primaria["principal"]))
update_geom_defaults("line",list(colour = pal_primaria["principal"]))
update_geom_defaults("line",list(colour = pal_primaria["principal"]))
update_geom_defaults('smooth',list(colour = pal_primaria["principal"]))
update_geom_defaults("boxplot",list(fill = pal_primaria["principal"],
                                    col = pal_primaria["ponto"]))
update_geom_defaults("point",list(colour = pal_primaria["ponto"]))
update_geom_defaults("label",list(colour = "white",
                                  fill = pal_primaria['ponto'])
                     )
update_geom_defaults("errorbar",
                     list(colour = pal_primaria["principal"],
                          linetype = 2)
                     )

# cores para categóricas, 'fill' e 'color'
plot_fill_cat <- scale_fill_discrete(palette = pal_complementar)
plot_color_cat <- scale_color_discrete(palette = pal_complementar)

# cores para contínuas, 'fill' e 'color'
plot_fill_cont <- scale_fill_gradient(high = '#006401',
                                      low = '#CDFFCC')

plot_color_cont <- scale_colour_gradient(high = '#006401',
                                         low = '#CDFFCC')


# cores para contínuas, 'fill' e 'color'
plot_fill_cont2 <- scale_fill_gradient(high = '#0000AA',
                                      low = '#FFFFFF')

plot_color_cont2 <- scale_colour_gradient(high = '#0000AA',
                                         low = '#FFFFFF')


# configurações de tema e legenda
plot_config <-
  theme_minimal() +
  theme(text = element_text(size = txt_size),
        legend.position = "bottom",
        legend.direction = "horizontal")

# 'NOVO' ggplot para escala de cores categóricas
ggplot_cat <- function(...){
  ggplot(...) +
    plot_fill_cat +
    plot_color_cat +
    plot_config
}


# 'NOVO' ggplot para escala de cores contínuoas
ggplot_cont <- function(...){
  ggplot(...) +
    plot_fill_cont +
    plot_color_cont +
    plot_config
}


# 'NOVO' ggplot para escala de cores contínuoas
ggplot_cont2 <- function(...){
  ggplot(...) +
    plot_fill_cont2 +
    plot_color_cont2 +
    plot_config
}

# 'NOVO' ggplot para escala de cores contínuoas
likert_values <- c("Discordo totalmente" = "#CA0020",
                   "Discordo" = "#F4A582",
                   "Não concordo nem discordo" =  "#F7F7F7",
                   "Concordo" = "#92C5DE",
                   "Concordo totalmente" = "#0571B0")


ggplot_likert <- function(...){
  ggplot(...) +
    # scale_fill_brewer(palette = "RdBu") +
    # scale_color_brewer(palette = "RdBu")+
    scale_fill_manual(values = likert_values) +
    plot_config
}

# 'NOVO' ggplot que detecta automaticamente o tipo de escla
ggplot_new <- function(...){

  # 1. Abrimos um dispositivo gráfico "fantasma" que não abre janela no Windows/Mac/Linux
  pdf(NULL)

  # Usamos o on.exit para garantir que o dispositivo feche, não importa o que aconteça
  on.exit(if (!is.null(dev.list())) dev.off())

  # p <- ggplot(...)

  p_out <-
    tryCatch({
      p1 <- ggplot_cat(...)
      print(p1)
      return(p1)
    },
    error = function(e){
      p2 <- ggplot_cont(...)
      return(p2)
    }
    )

  return(p_out)
}




# função para contar figura
fig_id <- 0
num_fig <- function(title){
  fig_id <<- fig_id + 1
  txt <- paste0("\n\n###### Figura ",fig_id," - ",title,"\n\n")
  cat(txt)
}

# função para contar tabela
tb_id <- 0
num_tb <- function(title){
  tb_id <<- tb_id + 1
  txt <- paste0("\n\n###### Tabela ",tb_id," - ",title,"\n\n")
  cat(txt)
}


# função para quebra de textos em gráficos (chatGPT)
quebra_texto2 <- function(texto, largura_max = 40, aplicar_se_maior = 40) {

  if (nchar(texto) <= aplicar_se_maior) {
    return(texto)
  }

  return(paste(strwrap(texto, width = largura_max), collapse = "\n"))
}
