library("data.table")
library("ggplot2")
library("magrittr")
library("tikzDevice")
library("yaml")
library("RColorBrewer")
library("knitr")
library("treemap")
library("ggfittext")
library("ggalluvial")

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




create_hash_tree <- function(
    file_path_bb_agg_pro = paste0(path_building_block_data,"/building_blocks_agg/","BB_agg_pro_11565019-12964999.csv.gz"),
    file_path_bb_lookup = paste0(path_building_block_data,"/building_blocks_agg/","BB_lookup_11565019-12964999.csv.gz"),
    outpath_hash_tree = NULL, outpath_protocol_tree  = NULL){
  
  # read bb lookup table
  BB_lookup_table <- read.csv(file_path_bb_lookup, sep = ",", stringsAsFactors = FALSE, colClasses = "character") %>%
    data.table() %>%
    .[, count := sapply(count,as.numeric)] %>%
    .[, subtraces_N := sapply(subtraces_N,as.numeric)] %>%
    .[order(count, decreasing = TRUE)]

  # read building blocks 
  BB_agg_pro <- read.csv(file_path_bb_agg_pro, sep = ",", stringsAsFactors = FALSE, colClasses = "character") %>%
    data.table() %>%
    .[, count := sapply(count,as.numeric)] %>%
    .[, subtraces_N := sapply(subtraces_N,as.numeric)] %>%
    .[order(count, decreasing = TRUE)]
  
  # define transition table from one from_hash (sub_hash) to its contained to_hashes (hashes)
  trans <- BB_lookup_table[, .(to_hash = hashes, to_a = appearHash), .(from_hash = sub_hash, isSubtrace)]
  rm(BB_lookup_table)
  trans %<>%  merge(x = ., y = BB_agg_pro[(isSubtrace == "True"), to_protocol, .(to_hash = sub_hash)], all.x = TRUE, by = c("to_hash")) # merge to_protocol from to_hash
  

  # construct linking structure of bb_hash (sub_hash) -> building block tree
  
  depth_i = 0 # start with depth = 0 : external transaction
  appearHash <- 1 # default: to enter loop
  
  # for each external transaction pattern to a protocol: sub_hash
  hash_tree <-  BB_agg_pro %>% # init
    #.[(to_protocol == "1inch")] %>%
    .[(isSubtrace == "False")]   %>%  .[,.(sub_hash = sub_hash,
                                           to_hash = sub_hash,
                                           depth = depth_i,
                                           to_protocol = to_protocol,
                                           ext_protocol = to_protocol,
                                           ids = to_protocol)] 
  hash_tree %<>% # add counts of BB for external calls
    merge(x = ., y = BB_agg_pro[(isSubtrace == "False"), .(value = count), .(sub_hash, ext_protocol = to_protocol)], all.x = TRUE,  by = c("sub_hash","ext_protocol")) 
  system.time({
    while(appearHash>0){
      print(paste0("depth: ",depth_i))
      # use transition-table
      if(depth_i < 1){ # at first: : external
        t <- trans[isSubtrace == "False",.(to_hash,from_hash,to_a,to_protocol)]
      }else{ # then internal blocks
        t <- trans[isSubtrace == "True",.(to_hash,from_hash,to_a,to_protocol)]
      }
      
      # increase index
      depth_i <- depth_i + 1
      
      # reduce list of last depth index
      hash_tree_prevLevel <- hash_tree[(depth == depth_i - 1)] %>%
        .[, .(sub_hash = sub_hash, depth = depth_i, from_hash = to_hash, from_protocol = to_protocol, ext_protocol = ext_protocol, parent = ids, value)]
      
      # merge next hashes to latest ones with the transition table
      setkey(hash_tree_prevLevel, from_hash)
      setkey(t, from_hash)
      hash_tree_nextLevel <- merge(hash_tree_prevLevel, t, all.x = TRUE, by = "from_hash")
      hash_tree_nextLevel[, ids := parent] # backup
      hash_tree_nextLevel[!is.na(to_a), # count number of appearing protocols
                          appearing_pro :=  (paste0(unique(sort(to_protocol)), collapse = ",")), 
                          by=.(from_hash, ext_protocol, sub_hash)]
      hash_tree_nextLevel[sapply(appearing_pro, function(ap){gregexpr(',', ap)[[1]][1]>=0}),
                          ids := paste0(ids,"_",appearing_pro)] # add appearing protocols if exists
      hash_tree_nextLevel[, ids := paste0(ids,"_",to_protocol)] # add protocol to ids
      hash_tree_nextLevel[!is.na(to_a), value := as.numeric(value) / as.numeric(to_a)]
      # number of further hashes -> to continue/stop loop
      appearHash <- hash_tree_nextLevel[, sum(as.numeric(to_a), na.rm = TRUE)]
      
      # bind new results to privious ones
      hash_tree %<>% rbind(., hash_tree_nextLevel[!is.na(to_a)],fill=TRUE) #
    }
  })
  
  # protocol tree for shiny app
  if(!is.null(outpath_protocol_tree)){
    
    protocol_tree <- hash_tree[,.(values = sum(value)),.(labels = to_protocol, ids, parents = parent, ext_protocol)] 
    write.csv2(protocol_tree, file = gzfile(outpath_protocol_tree))
    rm(protocol_tree)
  }
  
  # add count's for external calls to protocols
  hash_tree %<>% merge(x = ., y = BB_agg_pro[(isSubtrace == "False"), .(count), .(sub_hash, ext_protocol = to_protocol)], all.x = TRUE,  by = c("sub_hash","ext_protocol"))

  
  if(!is.null(outpath_hash_tree)){
    write.csv2(hash_tree, file = gzfile(outpath_hash_tree), sep = ";")
  }
  
  return(hash_tree)
}

