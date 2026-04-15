
# função 'arrumar_registro'
#' Tira caracteres não numéricos e imputa '0's à esquerda de um código identificador (cpf, cnpj, etc)
#' Usa a função 'str_pad' do pacote 'stringr'
#' @param x vetor com os códigos identificadores (cpf, cnpj, etc.)
#' @param digitos número de dígitos
#'
#' @return vetor com os códigos sem caracteres não numéricos, com 0's imputados à esquerda para completar
#' o número de dígitos especificados em 'digitos' 
#' @export
#'
#' @examples
#' 

## funções ----

# função para índice de diversificação de produção (SID)
indice_sid <- function(x){
  x_comp <- na.omit(x)
  if (length(x_comp) == 0){
    return(numeric(0))
  }else{
    w_i <- x_comp/sum(x_comp)
    return(1-sum(w_i^2))
  }
}

# função para o índice de dominância do produto principal 
indice_dom <- function(x){
  x_comp <- na.omit(x)
  if (length(x_comp) == 0){
    return(numeric(0))
  }else{
    w_i <- x_comp/sum(x_comp)
    return(max(w_i))
  }
}



find_key <- function(dt){
  dt_nm <- names(dt)
  dt_key <- character(0)
  n_dt <- NROW(dt)
  i <- 1
  n_distinct_dt <- 0
  while(n_distinct_dt < n_dt){
    dt_key <- c(dt_key,dt_nm[i])
    n_distinct_dt <- select(dt,one_of(dt_key)) %>% unique %>% NROW()
    if(length(dt_key) == length(dt_nm)){break()}
    i <- i + 1
  }
  return(dt_key)
}

relac_chaves <- function(nmdt_x,nmdt_y,nms_out = character(0)){
  dt_x <- get(nmdt_x) %>% copy; dt_y = get(nmdt_y) %>% copy()
  common_names <- intersect(names(dt_x),names(dt_y)) %>% setdiff(nms_out)
  dt_y[,x_in_y := 1]
  left_join(dt_x %>% select(common_names) %>% unique,
            dt_t %>% select(one_of(common_names),x_in_y) %>% unique(),
            by = common_names) %>%
    .[,.(base_x = nmdt_x,
         base_y = nmdt_y,
         chave = paste0(common_names,collapse = ";"),
         .N),
      .(x_in_y)] %>%
    select(base_x,base_y,chave,x_in_y,N)
}

trat_categorias <- function(charcol,enc_char = "UTF-8"){
  iconv(charcol,enc_char,"ASCII//TRANSLIT") %>%
    # gsub("\\/","",.) %>%
    gsub("-|/"," ",.) %>%
    gsub("[[:punct:]]","",.) %>%
    tolower() %>%
    removeWords(stopwords('pt')) %>%
    gsub("[[:space:]]{1,}","_",.)
}

cat2flag <- function(data,key_vars,var_nm,prefix = var_nm,return_original = T,...){
  # tudo igual a 1
  data[,aux_var := 1]
  
  # fórumla para o dcast na form key_vars1 + key_vars2 + ... ~ var_nm
  dcast_formulae <- paste0(paste0(key_vars,collapse = " + ")," ~ ",var_nm) 
  
  # dcast preenchendo vazios com zero
  data  %>%
    copy %>%
    # unique das variáveis envolvidas
    select(one_of(c(key_vars,var_nm)),aux_var) %>% 
    # unique() %>%
    dcast(dcast_formulae,
          value.var = "aux_var",
          fill = 0,
          ...) -> data_out
  
  # renomeando colunas das categorias com o prefixo estabelecido
  cast_cols <- setdiff(names(data_out),key_vars)
  new_cols <- paste0(prefix,"_",cast_cols)
  setnames(data_out,cast_cols,new_cols)
  
  data[,aux_var := NULL]
  
  # join com o data.frame original
  if(return_original){
    data_out <- left_join(data,data_out,by = key_vars)
    return(data_out)
  }else{
      return(data_out)
    }
}


arrumar_registro <- function(x,digitos = 11,numeric = T){
  # passo 1: tirar caracteres não numéricos
  y <- gsub("[[:alpha:]]|[[:punct:]]|[[:space:]]","",x)
  
  
  if(!numeric){
    # passo 2: completar com '0' à esquerda se for menor que 'digitos', caso não queira numérico
    z <- str_pad(y,digitos, pad = '0')
  }else{
    # passo3: caso contrário, retorna integer64
    as.integer64(y)
  }
}
#' função 'limpar_valores'
#' 'Limpa' caracteres especiais (como R$) de valores moretários/numéricos, retirando divisor de
#' milhar, fornecido pelo usuário, tratando divisor decimal e convertendo para numérico. Também
#' prevê a inserção de expressão regular informando representação de valor negativo.
#'
#' @param x vetor com os valores monetários e/ou numéricos de qualquer natureza que se quer ajustar
#' @param divmilhar caracter que separa os milhares. Se não houver um, manter como NULL
#' @param dec caracter que separa os decimais. Se não houver um, manter como NULL
#' @param negativo expressão regular que indica a representação de números negativos. 
#' Se não houver, manter NULL
#'
#' @return retorna um vetor numérico após a limpeza e conversão para numérico
#' @export
#'
#' @examples arrumar_valores(c("(R$ 222.395,350)","R$ 195.32","(R$ 20135.8)"), dec = ",",divmilhar = ".",negativo = "^\\(.*\\)$")
limpar_valores <- function(x, 
                           divmilhar = NULL,
                           dec = NULL,
                           negativo = NULL) {
  # passo 1: retirar divisor de milhares, se houver
  if(!is.null(divmilhar)) x <- gsub(divmilhar,"",x, fixed = T)
  
  # passo 2: substituir caracter de decimal por ".", se houver
  if(!is.null(dec)) x <- gsub(dec, ".", x, fixed = T)
  
  # passo 3: detectar expressão regular onde ocorre negativo
  if(!is.null(negativo)) x <- ifelse(grepl(negativo,x),paste0("-",x),x)
  
  # passo 3: tirar caracteres não numéricos
  y <- gsub("[[:alpha:]]|\\$|\\(|\\)|[[:space:]]", "", x)
  
  # passo 4: retornar em formatu numérico
  as.numeric(y)
}

# função 'verifica_cpf'
#' verifica se cpf é válido
#' Usa a função 'strsplit' e str_pad
#' @param x número CPF sem caracteres não numéricos (".","-", etc.)
#' 
#' @return retorna TRUE se o dígito verificador do CPF é válido.
#' @export
#'
#' @examples
verifica_cpf <- function(x){
  if(nchar(x) > 11|is.na(x)){
    return(FALSE)
  }else{
    
    x <- str_pad(x,11,pad='0')
    vec_sep <- as.numeric(unlist(strsplit(x,"")))
    verif_dig <- vec_sep[10:11]
    vec_sep <- vec_sep[1:9]
    
    # passo 1: primeiro digito
    y <- sum(vec_sep*(10:2))
    
    #resto
    y <-  y %% 11
    d1 <- ifelse(y %in% 0:1,0,11-y)
    
    cpf_ok <- d1 == verif_dig[1]
    
    # passo 2: segundo digito
    vec_sep <- c(vec_sep,d1)
    
    z <- sum(vec_sep*(11:2))
    #resto
    z <- z %% 11
    d2 <- ifelse(z %in% 0:1,0,11-z)
    
    cpf_ok <- cpf_ok & d2 == verif_dig[2]
    
    return(cpf_ok)
  }
}


verifica_cnpj <- function(x){
  if(nchar(x) > 14 | is.na(x)){
    return(FALSE)
  }else{
    
    x <- str_pad(x,14,pad='0')
    vec_sep <- as.numeric(unlist(strsplit(x,"")))
    verif_dig <- vec_sep[13:14]
    vec_sep <- vec_sep[1:12]
    
    # passo 1: primeiro digito
    y <- sum(vec_sep*(c(5:2,9:2)))
    
    #resto
    y <-  y %% 11
    d1 <- ifelse(y %in% 0:1,0,11-y)
    
    cpf_ok <- d1 == verif_dig[1]
    
    # passo 2: segundo digito
    vec_sep <- c(vec_sep,d1)
    
    z <- sum(vec_sep*(c(6:2,9:2)))
    #resto
    z <- z %% 11
    d2 <- ifelse(z %in% 0:1,0,11-z)
    
    cpf_ok <- cpf_ok & d2 == verif_dig[2]
    
    return(cpf_ok)
  }
}

### adiciona mes
addmes <- function(x,w){
  y <- Year(x)
  m <- month(x)
  m <- m + w
  y <- y +  trunc((m-1)/12)
  m <- m %% 12
  m <- ifelse(m %in% 0 , 12, m )
  return(as.Date(paste0(y,'-',m,'-',1)))
}









############################################
## Merge power


merge2wayOR <- function(d1,d2,idA,idB){
  # create index column in both data.tables
  d1[, idx1 := .I]
  d2[, idx2 := .I]
  
  mAB <- c(idA, idB)
  mA <- idA
  mB <- idB
  
  # inner join on idA and idB
  j1 <- d1[d2, .(idx1, idx2), on = mAB , nomatch = 0L]
  m1 <- unique(j1$idx1)
  m2 <- unique(j1$idx2)
  print('a')
  
  # inner join on idA
  j2 <- d1[!(idx1 %in% m1)][d2[!(idx2 %in% m2)], .(idx1, idx2), on = mA, nomatch = 0L]
  m1 <- append(m1, unique(j2$idx1))
  m2 <- append(m2, unique(j2$idx2))
  print('B')
  # inner join on idB
  j3 <- d1[!(idx1 %in% m1)][d2[!(idx2 %in% m2)], .(idx1, idx2), on = mB, nomatch = 0L]
  m1 <- append(m1, unique(j3$idx1))
  m2 <- append(m2, unique(j3$idx2))
  print('C')
  
  # combine results
  j4 <- rbindlist(
    list(
      AB = cbind(
        d1[idx1 %in% j1[, idx1]],
        d2[idx2 %in% j1[, idx2]]),
      A. = cbind(
        d1[idx1 %in% j2[, idx1]],
        d2[idx2 %in% j2[, idx2]]),
      .B = cbind(
        d1[idx1 %in% j3[, idx1]],
        d2[idx2 %in% j3[, idx2]])),
    fill = TRUE, 
    idcol = "match_on") %>% return
  
}



merge2wayOR_full <- function(d1,d2,idA,idB){
  # create index column in both data.tables
  d1[, idx1 := .I]
  d2[, idx2 := .I]
  
  mAB <- c(idA, idB)
  mA <- idA
  mB <- idB
  
  # inner join on idA and idB
  j1 <- d1[d2, .(idx1, idx2), on = mAB , nomatch = 0L]
  m1 <- unique(j1$idx1)
  m2 <- unique(j1$idx2)
  print('a')
  
  # inner join on idA
  j2 <- d1[!(idx1 %in% m1)][d2[!(idx2 %in% m2)], .(idx1, idx2), on = mA, nomatch = 0L]
  m1 <- append(m1, unique(j2$idx1))
  m2 <- append(m2, unique(j2$idx2))
  print('B')
  # inner join on idB
  j3 <- d1[!(idx1 %in% m1)][d2[!(idx2 %in% m2)], .(idx1, idx2), on = mB, nomatch = 0L]
  m1 <- append(m1, unique(j3$idx1))
  m2 <- append(m2, unique(j3$idx2))
  print('C')
  
  ##name ajust
  
  intersect_names <- intersect(names(d1),names(d2))
  setnames(d1,intersect_names,paste0(intersect_names,"_d1"))
  setnames(d2,intersect_names,paste0(intersect_names,"_d2"))
  
  # combine results
  rbindlist(
    list(
      AB = cbind(
        d1[idx1 %in% j1[, idx1]],
        d2[idx2 %in% j1[, idx2]]),
      A. = cbind(
        d1[idx1 %in% j2[, idx1]],
        d2[idx2 %in% j2[, idx2]]),
      .B = cbind(
        d1[idx1 %in% j3[, idx1]],
        d2[idx2 %in% j3[, idx2]]),
      d1 = cbind(
        d1[!(idx1 %in% m1)],
        d2[1,lapply(.SD,function(x) NA)]),
      d2 = cbind(
        d1[1,lapply(.SD,function(x) NA)],
        d2[!(idx2 %in% m2)])),
    fill = TRUE, 
    idcol = "match_on") %>% return
  
}



#' Title estruturar_string
#' Retorna um vetor de novos nomes estruturados, sem repetição, minúsculos, sem caracter especial e com "_" no lugar de "."
#'
#' @param x vetor de strings
#' @param encoding encogin da base, sendo na maiorria 'utf8', 'latin1' OU 'Windows-1252
#'
#' @return vetor com novos nomes padronizados
#' @export
#'
#' @examples
estruturar_string <- function(x, encoding = "utf8") {
  x %>%
    iconv(from = encoding, to = "ASCII//TRANSLIT") %>% # tira caracteres de a acentuação
    tolower %>% ## tudo em minúsculo
    gsub(pattern = "\\-", replacement = "_") %>% # substituindo "-" por "_"
    gsub(pattern = "[[:space:]]{1,}", replacement = "_") %>% # substituindo 1 ou mais espaços por "_"
    gsub(pattern = "\\_{2,}", replacement = "_") # tirando "_" múltilpos
}

#' Title estruturar_nomes
#' Retorna um vetor de novos nomes estruturados, sem repetição, minúsculos, sem caracter especial e com "_" no lugar de "."
#'
#' @param data data.frame ou data.table 
#' @param encoding encogin da base, sendo na maiorria 'utf8', 'latin1' OU 'Windows-1252
#'
#' @return vetor com novos nomes padronizados
#' @export
#'
#' @examples
estruturar_nomes <- function(data, encoding = "utf8"){
  colunas_validas <- data %>%
    names %>%
    estruturar_string(encoding) %>% # pré-estrutura tirando acento, ".", espaço, etc.
    make.names(unique = T) %>% # repetidos são concatenados a números
    gsub(pattern = "\\.", replacement = "_") %>% # substituindo "." por "_"
    gsub(pattern = "_{2,}",replacement = "_") # tirando "_" múltilpos
  
  ## retorna colunas estruturadas
  colunas_validas
}



#' Title função 'formatar_num': acrescenta big.mark = "." e decimal.mark = ",", de acordo com o número de dígitos escolhido.
#' função 'formatar_num': acrescenta big.mark = "." e decimal.mark = ",", de acordo com o número de dígitos escolhido.
#' @param x vetor ou número
#' @param digitos número de dígitos que irão aparecer
#'
#' @return
#' @export
#'
#' @examples
formatar_num <- function(x,digitos){
  formatC(x,
          big.mark = ".",
          decimal.mark = ",",
          format = "f",
          digits = digitos)
}

format_num_en <- function(x,digitos){
  formatC(x,
          big.mark = ",",
          decimal.mark = ".",
          format = "f",
          digits = digitos)
}

n_aas <- function (N, e, z, v = 1/4,prop = T, aloc = NULL) {
  N/((e/z)^2*(N - prop)/v + 1)
}

e_aae <- function(N, gamma,n, v = 1/4,prop = NULL, aloc = "unif",sample = F) {
  wh_pop  <- N/sum(N)
  if(sample){
    if(length(n) == 1) stop("n deve ser um vetor com os tamanhos observados em cada estrato")
    n_st <- n
  }else{
    if(aloc == "prop"){
      n_st <- n*wh_pop %>% ceiling()
    }else{
      n_st <- rep((n/length(N)) %>% ceiling(),length(N))
    }
  }
  
  frac_st <- n_st/N
  if(sample){
    v <- v*n_st/(n_st - 1)
  }
  v.vec <- (wh_pop^2)*v*(1 - frac_st)/(n_st - as.numeric(sample))
  s.vec <- sum(v.vec)
  z <- abs(qnorm((1-gamma)/2))
  e <- z*sqrt(s.vec)
  return(e)
}



n_aae <- function (N, e, gamma, v = 1/4,prop = NULL, aloc = "unif") {
  # pesos
  wh_pop  <- N/sum(N)
  z <- abs(qnorm((1-gamma)/2))
  
  # # comp. fração
  # n_num <- ifelse(aloc == "otima",
  #                 sum(sqrt(v)*wh_pop)^2,
  #                 sum(v*(wh_pop^2)/wh_samp)
  # )
  
  if(aloc == "prop"){
    vec_v <- v*wh_pop*(1*wh_pop)
    n_tot = sum(vec_v)*(z/e)^2
    n_strt <- ceiling(wh_pop*n_tot)
  }else{
    vec_v1 <- v*(wh_pop^2)
    vec_v2 <- vec_v1/N
    n_result <- sum(vec_v1)/((e/z)^2 + sum(vec_v2))
    n_strt <- rep(ceiling(n_result),length(N))
  }
  n_strt
}

n_aac <- function(N, e, z, v, prop = F, aloc = "unif"){
  N/((e/z)^2*(N - prop)/v + 1)
}


find_lat_long <- function(addr) {
  url = paste('http://maps.google.com/maps/api/geocode/xml?address=', 
              addr,'&sensor=false',sep='') 
  
  doc = xmlTreeParse(url) 
  root = xmlRoot(doc) 
  lat = xmlValue(root[['result']][['geometry']][['location']][['lat']]) 
  long = xmlValue(root[['result']][['geometry']][['location']][['lng']])
  silent = T
  data.table(endereco = addr,lat,long)
}

find_lat_long_ex <- function(addr){
  result <- try(find_lat_long(addr),silent = T)
  if(inherits(result,"try-error")){
    data.table(endereco = addr,lat = NA, long = NA)
  }else{
    result
  }
}



lista_e <- function(x,conexao = " e "){
  x <- unique(x)
  if(length(x) > 1){
    y <- paste(head(x,-1),collapse = ", ")
    z <- tail(x,1)
    paste(y,z,sep = conexao)
  }else{
    x
  }
}

prob_selec_ssu <- function(n,p,f2){
  seq_n <- 0:n
  exp_n <- (1-f2)^seq_n
  bin_n <- dbinom(seq_n,n,p)
  1 - sum(exp_n*bin_n)
}



lista_e <- function(x,connective = " and "){
  x <- unique(x)
  if(length(x) > 1){
    y <- paste(head(x,-1),collapse = ", ")
    z <- tail(x,1)
    paste(y,z,sep = connective)
  }else{
    x
  }
}


last_complete <- function(x){
  y = x
  
  if(length(x) > 1){
    for(i in 2:length(x)){
      if(is.na(x[i])) y[i] <- y[i-1]
    }
  }
  y
}

# pegando último valor, ou o mais frequente
most_freq <- function(x){
  if(any(!is.na(x))){
    y = na.omit(x)
    freqs <- table(y) 
    mode_y <- names(freqs)[which(freqs == max(freqs))]
    return(tail(y[as.character(y) %in% mode_y],1))
  }else{
    return(tail(x,1))
  }
}




# -----

knn_pessoas <- function(data_knn,
                        cols_knn,
                        class_knn,
                        app_data = NULL,
                        nb = NULL,
                        prop_test = 0.2,
                        incomp_sep = FALSE,
                        print_results = T) {
  stopifnot(is.data.table(data_knn),
            all(cols_knn %in% names(data_knn)),
            class_knn %in% names(data_knn))
  
  data_knn <- data_knn %>% copy
  
  # padronizando as variáveis (score z)
  cols_knn_pad <- paste0(cols_knn,"_pad")
  data_knn[,c(cols_knn_pad) := lapply(.SD,
                                      function(x){
                                        (x - min(x,na.rm = T))/(max(x,na.rm = T) - min(x,na.rm = T))
                                      }),
           .SDcols = cols_knn]
  
  # indicador de .NA
  data_knn[,obs_compl := complete.cases(data_knn %>% 
                                          select(
                                            one_of(
                                              c(cols_knn,
                                                class_knn)
                                            )
                                          )
  )]
  
  
  ### training X test data
  if(is.null(app_data)){
    set.seed(6516)
    data_knn_training <- 
      data_knn %>% 
      split(by = "int_rel_dist") %>%
      lapply(sample_frac,size = (1-prop_test)) %>%
      rbindlist
    data_knn_test <- setdiff(data_knn,data_knn_training)
    
    cat(
      paste0("\n\n ",
             data_knn[,sum(!obs_compl)],
             " incomplete data among ",
             NROW(data_knn),
             ", ",
             data_knn_training[,sum(!obs_compl)],
             " in training data, ",
             data_knn_test[,sum(!obs_compl)],
             " in test data.")
    )
  }else{
    stopifnot(is.data.table(app_data))
    
    data_knn_training <- data_knn
    app_data <- app_data %>% copy
    
    # padronizando variáveis, mas no test data fornecido
    app_data[,c(cols_knn_pad) := lapply(.SD,
                                        function(x){
                                          (x - min(x,na.rm = T))/(max(x,na.rm = T) - min(x,na.rm = T))
                                        }),
             .SDcols = cols_knn]
    
    # indicador de .NA no test data fornecido
    app_data[,obs_compl := complete.cases(app_data %>% 
                                            select(
                                              one_of(
                                                c(cols_knn,
                                                  class_knn)
                                              )
                                            )
    )]
    data_knn_test <- app_data
  }
  
  
  # >> rodando o knn nos datasets completos----
  
  if(incomp_sep){
    # training completo
    data_knn_training_comp <-
      data_knn_training %>%
      subset(obs_compl) %>%
      select(one_of(cols_knn)) 
    
    # test completo
    data_knn_test_comp <-
      data_knn_test %>%
      subset(obs_compl)
    
    # test imcompleto
    data_knn_test_incomp <- 
      data_knn_test %>%
      subset(!obs_compl)
    
    # classes do training completo
    class_knn.df <-
      data_knn_training %>%
      subset(obs_compl) %>%
      select(one_of(class_knn)) %>% 
      data.frame 
    
    if(is.null(nb)) nb <- ceiling(sqrt(NROW(data_knn_training_comp))/2)
    
    knn_assess <- 
      class::knn(train = data_knn_training_comp %>% 
                   data.frame ,
                 test  = data_knn_test_comp  %>%
                   select(one_of(cols_knn)) %>%
                   data.frame,
                 cl =    class_knn.df[,1],
                 k = nb,
                 prob = T)
    data_knn_test_comp$tipo_par_prev <- knn_assess
    data_knn_test_comp$tipo_par_prob <- attr(knn_assess,"prob")
  }else{
    # test imcompleto
    data_knn_test_incomp <- data_knn_test %>% copy
    data_knn_test_comp <- setdiff(data_knn_test,data_knn_test_incomp)
  }
  
  # >> rodando o knn nos datasets incompletos----
  
  knn_core <- 
    CoreModel(
      as.formula(
        paste0(
          class_knn,
          " ~ ",
          paste(cols_knn_pad,collapse = " + ")
        )
      ),
      data = data_knn_training,
      model = "knnKernel"
    )
  
  knn_eval_test <- predict(knn_core,data_knn_test_incomp)
  data_knn_test_incomp$tipo_par_prev <- knn_eval_test$class
  data_knn_test_incomp$tipo_par_prob <- knn_eval_test$probabilities %>% apply(1,max)
  
  ## juntando as duas partes
  data_knn_test <- list(data_knn_test_comp,
                        data_knn_test_incomp) %>% rbindlist(fill = T)
  
  ## resultados
  if(print_results & is.null(app_data)){
    
    cat(
      paste0("\n\n kNN uses ",nb," neighbors.")
    )
    
    if(incomp_sep){
      res_comp <- data_knn_test_comp[,100*mean(tipo_par != tipo_par_prev)]
      cat(paste0("\n\n",
                 "Nos dados completos, há ",
                 res_comp %>% formatar_num(2),
                 "% de erro de classificação.\n"))
      
      res_incomp <- data_knn_test_incomp[,100*mean(tipo_par != tipo_par_prev)]
      cat(paste0("\n\n",
                 "Nos dados incompletos, há ",
                 res_incomp %>% formatar_num(2),
                 "% de erro de classificação"))
      
      
      data_knn_test[,list(freq = .N),
                    by = c("obs_compl","tipo_par","tipo_par_prev")] %>%
        dcast(obs_compl + tipo_par ~ tipo_par_prev,
              value.var = "freq",
              fill = 0) %>% 
        knitr::kable(caption = "Cross validation - dados completos X incompletos") %>%
        print
      
      
    }
    
    res_total <- data_knn_test[,100*mean(tipo_par != tipo_par_prev)]
    cat(paste0("\n\n",
               "Em todo o test data, há ",
               res_total %>% formatar_num(2),
               "% de erro de classificação.\n\n"))
    
    data_knn_test[,list(freq = .N),
                  by = c("tipo_par","tipo_par_prev")] %>%
      dcast(tipo_par ~ tipo_par_prev,
            value.var = "freq",
            fill = 0) %>%  knitr::kable(caption = "Cross validation - todo o test data.") %>%
      print
    
    
    data_knn_test[,diff_class := tipo_par != tipo_par_prev]
    base_plot <- data_knn_test %>%
      ggplot(aes(y = tipo_par_prob))
    
    plot1 <- base_plot + geom_boxplot(aes(x = tipo_par_prev))
    
    plot2 <- base_plot + geom_boxplot(aes(x = tipo_par))
    
    plot3 <- base_plot + geom_boxplot(aes(x = diff_class))
    
    gridExtra::grid.arrange(plot1,plot2,plot3,ncol = 1)
    
  } 
  
  return(data_knn_test)
}


faz_metadados <- function(data){
  data.table(
    variavel = names(data),
    tipo = lapply(data,class) %>% unlist
  )
}

## tansform dtb in data.table
dtm2dt <- function(dtm,
                   transpose = T,
                   exclude_zeros = T,
                   tf_idf = T,
                   name_iddoc = NULL){
  dtm_mt <- as.matrix(dtm)
  dt_tb <- data.table(dtm_document = rownames(dtm),dtm_mt) 
  
  if(transpose){
    dt_tb <- 
      dt_tb %>%
      melt(id.vars = "dtm_document",
           value.name = "freq",
           variable.name = "term",
           na.rm = T)
    if(exclude_zeros){
      dt_tb <- dt_tb %>% subset(freq > 0) # excluding zeros
    }
    if(tf_idf){
      dt_tb <- 
        dt_tb %>% 
        .[,p_freq := freq/sum(freq), by = dtm_document] %>% # % frequency
        .[,term_docs := sum(freq > 0), by = term] %>%  # number of documents containing each term
        .[,idf := log(length(unique(dtm_document))/term_docs,2)] %>% # idf
        .[,`:=`(tfidf = freq*idf,
                tfidf_p = p_freq*idf)] # and, finally, tf_idf, both normalized and non-normalized 
    }
    
  }
  
  
  if(!is.null(name_iddoc)){
    setnames(dt_tb,"dtm_document",name_iddoc)
  }
  dt_tb
}


# Função seleciona PCA's que explicam 100Xp% das variáveis
reduct_pca <- function(pca_obj,p){
  # selecionando núm. de PC (= 75% da variância)
  k_pca <- 
    min(which(
      cumsum(pca_obj$sdev^2/sum(pca_obj$sdev^2)) >= p)
    )
  pca_obj$x[,1:k_pca]
}


min_data <- function(x){
  stopifnot(class(x) == "Date")
  if(all(is.na(x))){
    return(as.Date(NA))
  }else{
    return(min(x,na.rm = T))
  }
}

max_data <- function(x){
  stopifnot(class(x) == "Date")
  if(all(is.na(x))){
    return(as.Date(NA))
  }else{
    return(max(x,na.rm = T))
  }
}

## função retorna grafos tcm e grafos
tcm_grafos <- function(dt){
  
  
  tokens_lgbt = space_tokenizer(dt)
  
  # Create vocabulary. Terms will be unigrams (simple words).
  it_lgbt = itoken(tokens_lgbt, progressbar = FALSE)
  vocab_lgbt = create_vocabulary(it_lgbt,ngram = c(1L,2L))
  vocab_lgbt = prune_vocabulary(vocab_lgbt, term_count_min = 3L)
  
  
  # Use our filtered vocabulary
  vectorizer_lgbt = vocab_vectorizer(vocab_lgbt)
  
  # use window of 5 for context words (TCM = term-co-occurrence matrix)
  tcm_lgbt = create_tcm(it_lgbt, vectorizer_lgbt, skip_grams_window = 10L)
  
  # verificando 20 palavras com maior TCM para cada termo identidade lgbtqia+
  lapply(occor_lgbt %>%
           gsub(" ","_",.),
         function(x)try(tcm_lgbt[x,] %>% sort(decreasing = T) %>% head(20))) -> lgbt_20tcm
  names(lgbt_20tcm) <- occor_lgbt
  lgbt_20tcm
  
  # verificando 20 palavras com maior TCM para cada termo lgbtfobia
  lapply(occor_lgbtfobia %>%
           gsub(" ","_",.),function(x)try(tcm_lgbt[x,] %>% sort(decreasing = T) %>% head(20))) -> lgbtfobia_20tcm
  names(lgbtfobia_20tcm) <- occor_lgbtfobia
  lgbtfobia_20tcm
  
  # verificando 20 palavras com maior TCM para cada termo ofensivo
  lapply(occor_ofensas %>%
           gsub(" ","_",.),function(x)try(tcm_lgbt[x,] %>% sort(decreasing = T) %>% head(20))) -> ofensas_20tcm
  names(ofensas_20tcm) <- occor_ofensas
  ofensas_20tcm
  
  # data.frame com os links entre a palavra e os 20 maiores TCM
  list_links_tcm <- 
    lapply(ls(pattern = "20tcm$"),function(x){
      list_tcm <- get(x) %>% unlist
      tudo_errado <- lapply(list_tcm,inherits, what = 'try-error') %>% all()
      if(!tudo_errado){
        data.table(from = names(list_tcm) %>% gsub("\\..*","",.),
                   to = names(list_tcm) %>% gsub(".*\\.","",.),
                   value = as.numeric(list_tcm),
                   group = x) %>%
          na.omit() %>%
          filter(value > 0)
        
        
      }else{
        NULL
      }    
    })
  
  links_tcm <- list_links_tcm %>% rbindlist(fill = T) %>% na.omit() %>% unique()
  
  # apply(dtm_lgbt_no_os %>% as.matrix,2,sum) -> vocab_dtm
  
  # data.frame com as frequências dos termos
  list_nodes_tcm <- 
    lapply(list_links_tcm,function(ltcm){
      all_nodes <- c(ltcm$from,ltcm$to) %>% unique
      nodes_tcm <- data.table(nodes_x = all_nodes,
                              size = vocab[all_nodes])
      nodes_tcm <- left_join(nodes_tcm,ltcm %>% select(from) %>% unique,by = c("nodes_x" = "from"))
      nodes_tcm[,label_nodes := nodes_x]
      nodes_tcm[,ingroup := as.numeric(nodes_x %in% ltcm$from)]
      nodes_tcm
    })
  nodes_tcm <- rbindlist(list_nodes_tcm,fill  = T) %>% select(-ingroup) %>% unique()
  nodes_tcm[,ingroup := as.numeric(nodes_x %in% links_tcm$from)]
  
  # visualizando links
  # formato de rede (função 'network')
  pinf_net2 <- graph_from_data_frame(d = links_tcm ,
                                     vertices = nodes_tcm  ,
                                     directed = F)
  
  list_pinf_net2  <- 
    lapply(1:length(list_links_tcm),function(i){
      li_tcm <- list_links_tcm[[i]]
      no_tcm <- list_nodes_tcm[[i]]
      if(NROW(li_tcm) > 0){
        graph_from_data_frame(d = li_tcm,
                              vertices = no_tcm,
                              directed = F)
      }else{
        return(NULL)
      }
    })
  return(list(tcm_lgbt = tcm_lgbt,
              links_tcm = links_tcm,
              links_tcm = links_tcm,
              pinf_net2 = pinf_net2,
              list_pinf_net2 = list_pinf_net2))
}

split_assunto <- function(dt,id_cols = NULL){
  ## totais por assunto
  sep_assunto_b2 <- str_split(dt$id_assunto,"\\,",simplify = T)
  if(is.null(id_cols)){
    id_cols <- setdiff(names(dt),"id_assunto")
  }
  b2_assunto.dt <- data.table(dt,sep_assunto_b2) %>%
    melt(id.vars = c(id_cols),
         measure.vars = paste0("V",1:NCOL(sep_assunto_b2)),
         value.name = "cod_assunto",
         na.rm = T) %>%
    filter(cod_assunto != "")  %>%
    .[,cod_assunto := gsub("\\{|\\}",'',cod_assunto) %>% as.integer]  %>%
    left_join(assunto %>% select(id,nome,nome_completo),
              by = c("cod_assunto" = "id"))
  
  # captando a matéria principal
  b2_assunto.dt[,materia := gsub("\\|.*",
                                 "",
                                 nome_completo,ignore.case = T) %>%
                  str_trim]
  b2_assunto.dt
}

split_classe <- function(dt,id_cols = NULL){
  ## totais por classe
  sep_classe_b2 <- str_split(dt$id_classe,"\\,",simplify = T)
  if(is.null(id_cols)){
    id_cols <- setdiff(names(dt),"id_classe")
  }
  b2_classe.dt <- data.table(dt,sep_classe_b2) %>%
    melt(id.vars = c(id_cols),
         measure.vars = paste0("V",1:NCOL(sep_classe_b2)),
         value.name = "cod_classe",
         na.rm = T) %>%
    filter(cod_classe != "")  %>%
    .[,cod_classe := gsub("\\{|\\}",'',cod_classe) %>% as.integer]  %>%
    left_join(classe %>% select(id,nome,nome_completo),
              by = c("cod_classe" = "id"))
  
  # captando a matéria principal
  b2_classe.dt[,materia := gsub("\\|.*","",nome_completo,ignore.case = T)]
  b2_classe.dt
}

outlier_kd <- function(x){
  q1 <- quantile(x,0.25,na.rm = T)
  q3 <- quantile(x,0.75,na.rm = T)
  difq <- q3-q1
  li <- q1 - 5*difq
  lf <- q3 + 5*difq
  x < li | x > lf
}
# função de limpeza de um corpus
limpa_cp <- function(cp,more_stop = character(0)){
  cp  %>% 
    
    # tudo em letras minúsculoas
    tm_map(content_transformer(tolower)) %>% 
    
    # removendo pontuação
    tm_map(removePunctuation) %>% 
    
    # removendo números
    # tm_map(removeNumbers)
    
    # removendo acentuação
    tm_map(content_transformer(function(x){
      iconv(x,"utf8","ASCII//TRANSLIT")
    })) %>%
    
    # removendo "'"
    tm_map(content_transformer(function(x){
      gsub("\'","",x)
    })) %>%
    
    # removendo 'stopwords'
    tm_map(removeWords,c(stopwords("pt"),
                         more_stop)
           ) %>% 
    
    # removendo espaço em branco extra
    tm_map(stripWhitespace)

}


# função de limpeza de vetor de caracterers
limpa_txt <- function(txt,more_stop = character(0)){
  txt %>%
    
    # tudo em letras minúsculoas
    tolower %>% 
    
    # removendo pontuação
    removePunctuation %>% 
    
    # removendo números
    # tm_map(removeNumbers)
    
    # removendo acentuação
    iconv("utf8","ASCII//TRANSLIT") %>%
    
    # removendo "'"
    gsub("\'","",.) %>%
    
    # removendo 'stopwords'
    removeWords(c(stopwords("pt"),
                  more_stop)) %>% 
    
    # removendo espaço em branco extra
    stripWhitespace %>% 
    
    # removendo espaços em branco no início e no fim
    str_trim()
    
}


abrir_arquivo <- function(nm){
  dir_arquivo <- file.path(dir_textos,nm)
  cmd_abrir <- paste0("gedit '",dir_arquivo,"'")
  system(cmd_abrir,wait = F)
  Sys.sleep(1)
  # dir_arquivo
}


abrir_arquivo_dir <- function(dir,nm){
  dir_arquivo <- file.path(dir,nm)
  cmd_abrir <- paste0("gedit '",dir_arquivo,"'")
  system(cmd_abrir,wait = F)
  Sys.sleep(1)
  # dir_arquivo
}


wrd_substr <- function(txt,ini, fim){
  str_split(txt,boundary('word')) %>%
    unlist %>%
    substr(ini,fim) %>% 
    paste0(collapse = " ") -> tyt
  return(tyt)
}


# função para quebra de textos em gráficos (chatGPT)
quebra_texto2 <- function(texto, largura_max = 40, aplicar_se_maior = 40) {
  
  if (nchar(texto) <= aplicar_se_maior) {
    return(texto)
  }
  
  return(paste(strwrap(texto, width = largura_max), collapse = "\n"))
}
