# Title: Metadata generator
# Description: Generates metadata as postgresql comments
# Date: Mon Jun  4 10:51:35 2018
# Author: Javier Perez-Saez



# Preamble ---------------------------------------------------------------
library(DBI)
library(yaml)


# Functions ---------------------------------------------------------------

writeTableMetadataDB <- function(con, schema, table, mdata, 
                                 # metadata structure tempalte
                                 template = list(abstract = "",
                                                 comments = "",
                                                 "table insert" = list(time = as.character(Sys.time()), 
                                                                       script = "", 
                                                                       "by user" = ""),
                                                 "data origin" = list("original name" = "",
                                                                      "date created" = "", 
                                                                      "production script" = "",
                                                                      "producer" = list(name = "", contact = list(
                                                                        name = "",
                                                                        organisation = "",
                                                                        address = "",
                                                                        email = "",
                                                                        phone = ""
                                                                      ))),
                                                 "data usage restrictions" = "",
                                                 liscence = "",
                                                 "about" = "This metadata file is in yaml format!"), 
                                 gui = F){
  
  
  
  # fill with provided information
  metadata <- template
  
  # flatten both lists for filling
  mdata.f <- c(mdata, recursive = T)
  metadata.f <- c(metadata, recursive = T)
  
  # fill data
  for(n in names(mdata.f)){
    it <- try(metadata.f[n], silent = T)
    if (!inherits(it, "try-error")) {
      metadata.f[n] <- mdata.f[n]
    } else {
      warning(sprintf("Couldn't find field '%s' in metadata template", n))
    }
  }
  
  # get structure 
  lev_names <- str_split(names(metadata.f), "\\.")
  # insert data
  for(l in seq(lev_names)) {
    str <- str_c("metadata", str_c(lapply(lev_names[[l]], function(x) str_c("[[\"", x, "\"]]")), collapse =""), " <- metadata.f[l]")
    eval(parse(text = str))
  }
  
  comment <- as.yaml(metadata)  %>% str_replace_all(c("'" = ""))
  # insert metadata as comment in table
  dbSendStatement(con, str_c("COMMENT ON TABLE ", schema, ".", table, " IS '", comment, "';"))
}



