library("data.table")
library("ggplot2")
library("magrittr")
library("tikzDevice")
library("yaml")
library("RColorBrewer")
library("knitr")
library("treemap")
library("ggfittext")

# set working directory
#setwd("...")

# import config file
con <- file("./config.yaml", "r"); config <- read_yaml(con); close(con);
# create pathes
path_building_block_data = config$files$path_building_block_data
path_preprocess_data = config$files$path_preprocess_data
path_tables = config$tables$path_tables
path_figures = config$figures$path_figures
path_prot_colors = config$files$path_prot_colors

preprocess_bb <- function(
    file_path_bb_agg_info = paste0(path_building_block_data,"/building_blocks_agg/BB_agg_11565019-12964999_all.csv.gz"),
    out_path_bb_agg_pro = paste0(path_building_block_data,"/building_blocks_agg/","BB_agg_pro_11565019-12964999.csv.gz"),
    out_path_bb_lookup = paste0(path_building_block_data,"/building_blocks_agg/","BB_lookup_11565019-12964999.csv.gz")
){
  
  print(paste("read: ",file_path_bb_agg_info))
  BB_agg_add_info <- read.csv2(file_path_bb_agg_info, sep = ",", stringsAsFactors = FALSE, colClasses = "character") %>% #BB_agg_all_all
    data.table() %>%# .[, X := NULL] %>%
    .[, count := sapply(count,as.numeric)] %>%
    .[, subtraces_N := sapply(subtraces_N,as.numeric)] %>%
    .[order(count, decreasing = TRUE)]
  
  # reduce by removing columns (regarding external protocol calls)
  ## only keep abstracted values (Assets, hashes)
  print("agg: reduce additional information")
  BB_agg_pro <- BB_agg_add_info %>%
    .[, .(count = sum(count)),
      by=.(to_protocol, MethodId, MethodName, isSubtrace, subtraces_N, subtraces, subtraces_hash, addresses_sub, addresses_sub_hash, sub_hash)] %>%
    .[order(count, decreasing = TRUE)]
  
  rm(BB_agg_add_info)
  
  
  # number of hashes (building blocks) appearing; hashes are the pointers to further sub-blocks
  print("compute: number of sub-bb")
  BB_agg_pro %>% .[, appearHash := sapply(addresses_sub_hash, function(s){ lengths(regmatches(s, gregexpr("#", s)))})]
  
  
  # if sub-building blocks exist, extract them to vector_ssh
  print("split-up: all sub-bb pointers for each bb")
  BB_conv_temp <- BB_agg_pro[appearHash>0] %>%
    .[, vec_ssh := sapply(addresses_sub_hash, function(ssh){
      vec_ssh <- ssh %>% strsplit(x=., split = "_") %>% unlist()
      vec_ssh[sapply(vec_ssh, function(x){gregexpr(text = x, "#")>=0}) %>% as.vector()] %>%
        sapply(., function(x){gsub(x = x, pattern = "#", replacement = "")})
    })]
  
  # expand by unlisting the vector
  print("expand: table, each sub-bb pointer in an own row")
  col_storage = colnames(BB_conv_temp) # store column names
  BB_lookup_table <- BB_conv_temp[, .(hashes = unlist(vec_ssh)), by=setdiff(col_storage,"vec_ssh")] # expand
  
  # add protocol name of building block to hash
  print("add: protocol name to each bb")
  BB_lookup_table %<>%
    merge(x = ., y = unique(BB_agg_pro[isSubtrace=="True",.(appearPro = to_protocol),.(hashes = sub_hash)]),
          all.x=TRUE, by = "hashes" )
  
  # (restore by) collaps protocols to string 
  BB_conv_temp <- BB_lookup_table %>%
    .[, .(hashes = paste0(appearPro, collapse = "_")), by=setdiff(col_storage,"vec_ssh")]
  
  BB_lookup_table %>% write.csv(file = gzfile(out_path_bb_lookup))
  rm(BB_lookup_table)
  
  # create unique, ordered set of protocol string
  BB_conv_temp[, appearPro := sapply(hashes, FUN = function(h){
    x<-unique(strsplit(h,"_")[[1]]);  x[order(x)] %>% paste0(collapse = ",")})]
  
  # add appearing protocols
  print("add: appearing protocol info to original table")
  BB_agg_pro %<>%
    merge(x = ., y = BB_conv_temp, all.x = TRUE, by = colnames(BB_agg_pro)) %>%
    .[order(count, decreasing = TRUE)]
  
  # count number of protocols appearing in each set
  print("count: number of appearing protocols")
  BB_agg_pro %>%
    .[!is.na(appearPro), appearPro_N := sapply(appearPro, function(s){1+ lengths(regmatches(s, gregexpr(",", s)))})]
  BB_agg_pro %>%
    .[is.na(appearPro), appearPro_N := 0]
  
  
  BB_agg_pro %>% write.csv(file = gzfile(out_path_bb_agg_pro))
  
}
