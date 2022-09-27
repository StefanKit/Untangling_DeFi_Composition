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

# data preprocessing needed
#source("./scripts/building_blocks_processing.R")
#source("./scripts/building_blocks_preprocess_tree.R")

### plot

# function to create tikz from plot
TikzFromPlot <- function(plot, name = "test.tex", width = 3, height = 2){

  # enables using e.g. #, %
  options(
    tikzSanitizeCharacters = c('%','$','}','{','^','_','#','&','~'),
    tikzReplacementCharacters = c('\\%','\\$','\\}','\\{','\\^{}','\\_{}',
                                  '\\#','\\&','\\char`\\~')
  )

  tikz(file = name,
       standAlone=F, width = width, height = height, sanitize = TRUE)
  print(plot)
  #Necessary to close or the tikxDevice .tex file will not be written
  dev.off()
}

create_tree_map_plot <- function(
    file_path_bb_agg_pro = paste0(path_building_block_data,"/building_blocks_agg/","BB_agg_pro_11565019-12964999.csv.gz"),
    file_hash_tree = paste0(path_building_block_data,"/building_blocks_agg/","hash_tree_11565019-12964999.csv.gz"),
    outpath_treemap = NULL){
  
  # read building blocks 
  BB_agg <- read.csv(file_path_bb_agg_pro, sep = ",", stringsAsFactors = FALSE, colClasses = "character") %>%
    data.table() %>%
    .[, count := sapply(count,as.numeric)] %>%
    .[, subtraces_N := sapply(subtraces_N,as.numeric)] %>%
    .[order(count, decreasing = TRUE)]
  
  # read hash tree 
  hash_tree <- read.csv(file_hash_tree, sep = ";", stringsAsFactors = FALSE, colClasses = "character") %>%
    data.table() %>%
    .[, count := sapply(count,as.numeric)] %>%
    .[order(count, decreasing = TRUE)]
  
  
  # count protocols for first depth level
  dt_0 <- hash_tree[depth == 0, .(from_hash = to_hash), .(sub_hash, ext_protocol)] ## start with entering point (level zero, depth=0): for each protocols, it's bb
  count_pro_depth1 <- dt_0 %>% # add count of blocks
    merge(x = ., y = BB_agg[(isSubtrace == "False"), .(count), .(sub_hash, ext_protocol = to_protocol)], all.x = TRUE,  by = c("sub_hash","ext_protocol")) #%>% 
  dt_1_pro <- hash_tree[depth == 1, .(pro_decom = paste0(unique(sort(to_protocol)), collapse = ",\n")), by=.(from_hash, ext_protocol)]
  count_pro_depth1 %<>% # collaps protocols from sub-bb to single-liner
    merge(x = ., y = dt_1_pro, all.x = TRUE,  by = c("from_hash","ext_protocol") ) 
  count_pro_depth1[!is.na(pro_decom), appearPro_N := sapply(pro_decom, function(s){1+ lengths(regmatches(s, gregexpr(",", s)))})]
  count_pro_depth1[is.na(pro_decom), `:=` (pro_decom = "NONE", appearPro_N = 0)]
  
  tree_depth0 <- count_pro_depth1 %>% .[ext_protocol == "1inch"] %>% 
    .[, .(count = sum(count)), .(ext_protocol, pro_decom, appearPro_N)] %>% 
    .[order(count, decreasing = TRUE)] %>% 
    treemap(. , index=c("appearPro_N","pro_decom"), vSize="count", type="index") 
  
  tm_plot_data <- tree_depth0$tm %>% as.data.table() %>% 
    # calculate end coordinates with height and width
    .[, `:=` (x1 = x0 + w,  y1 = y0 + h) ]%>% 
    # get center coordinates for labels
    .[, `:=` (x = (x0+x1)/2, y = (y0+y1)/2) ] %>% 
    # mark primary groupings and set boundary thickness
    .[, `:=` (primary_group = ifelse(is.na(pro_decom), 1.2, .5)) ] %>% 
    # remove colors from primary groupings (since secondary is already colored)
    .[, `:=` (color = ifelse(is.na(pro_decom), NA, color))]
  
  plot_depth0 <- tm_plot_data %>% 
    ggplot(., aes(xmin = x0, ymin = y0, xmax = x1, ymax = y1)) + 
    # add fill and borders for groups and subgroups
    geom_rect(aes(fill = color, size = primary_group),
              show.legend = FALSE, color = "black", alpha = .3) +
    scale_fill_identity() +
    # set thicker lines for group borders
    scale_size(range = range(tm_plot_data$primary_group)) +
    # add labels
    ggfittext::geom_fit_text(aes(label = pro_decom), min.size = 5) +
    # options
    scale_x_continuous(expand = c(0, 0)) +
    scale_y_continuous(expand = c(0, 0)) +
    theme_void()
  

  
  if(!is.null(outpath_treemap)){
    
    #file.path(paste0(path_figures,"/treemaps/","1inch",".pdf"))
    ggsave(filename=outpath_treemap, 
           plot=plot_depth0,
           width = 5.5, height = 2.5, units = "cm", dpi = 300, scale = 2)
  }
  
  
  return(plot_depth0)
  
 
}

create_heatmap_plot <- function(
    file_path_bb_agg_pro = paste0(path_building_block_data,"/building_blocks_agg/","BB_agg_pro_11565019-12964999.csv.gz"),
    file_hash_tree = paste0(path_building_block_data,"/building_blocks_agg/","hash_tree_11565019-12964999.csv.gz"),
    outpath_heatmap = NULL){

  
## heatmap plot
  
  # read building blocks 
  BB_agg <- read.csv(file_path_bb_agg_pro, sep = ",", stringsAsFactors = FALSE, colClasses = "character") %>%
    data.table() %>%
    .[, count := sapply(count,as.numeric)] %>%
    .[, subtraces_N := sapply(subtraces_N,as.numeric)] %>%
    .[order(count, decreasing = TRUE)]
  
  # read hash tree 
  hash_tree <- read.csv(file_hash_tree, sep = ";", stringsAsFactors = FALSE, colClasses = "character") %>%
    data.table() %>%
    .[, count := sapply(count,as.numeric)] %>%
    .[order(count, decreasing = TRUE)]
  
  
  addr = read.csv2(file.path(paste0(path_preprocess_data, '/Protocol_Addresses.csv')), sep = ",", stringsAsFactors = FALSE) %>% data.table()
  colors_addr <- read.csv2(file.path(paste0(path_prot_colors,"/protocol_colors.csv")), sep = ";", stringsAsFactors = FALSE) %>% as.data.table() %>% .[,X:=NULL]

  
  count_pro_decom <- hash_tree[depth == 0, .(to_hash), .(sub_hash, ext_protocol)] %>% 
    merge(x = ., y = BB_agg[(isSubtrace == "False"), .(count), .(sub_hash, ext_protocol = to_protocol)], all.x = TRUE,  by = c("sub_hash","ext_protocol")) %>% 
    merge(x = ., y = hash_tree[depth>0, .(pro_decom = paste0(unique(to_protocol), collapse = ",")), by=.(sub_hash, ext_protocol)], all.x = TRUE,  by = c("sub_hash","ext_protocol") ) %>% 
    .[, .(count = sum(count)) , by =  .(ext_protocol, pro_decom)]
  count_pro_decom[is.na(pro_decom), pro_decom := "NONE"]
  
  # extract set of names from string protocol decomposition
  count_pro_decom[, `set_names` := sapply(X = pro_decom, FUN = function(s){
    s %>% 
      gsub(x = ., pattern = " ", replacement = "", fixed = TRUE) %>% 
      strsplit(x = ., split = ",") %>% unlist()
  })]
  count_pro_decom[, `N_txs_byExtPro` := sum(count), by=.(ext_protocol)] # number of external transactions
  
  N_pro_byExt = count_pro_decom %>% 
    .[, .(protocol = unlist( set_names)) , by = .(ext_protocol, transaction_hash = count,N_txs_byExtPro) ] %>% #, N_txs_byExtPro
    .[, .(transaction_count = sum(transaction_hash)), by = .(ext_protocol, protocol,N_txs_byExtPro)] 
  
  
  heat_N_pro_byExt <-N_pro_byExt %>% 
    .[, .(100*transaction_count/N_txs_byExtPro), by=.(protocol,ext_protocol)] %>% 
    dcast.data.table(data = ., formula = ext_protocol~protocol, value.var = "V1") %>% 
    melt.data.table(data = ., id.vars = "ext_protocol", variable.name = "protocol", value.name = "V1")
  
  heat_plot <- function(data, xl = "ext", yl = "pro"){
    
    data %<>% merge(.,unique(addr[,.(protocol,ext_protocol_type = protocol_type)]), 
                    by.x = "ext_protocol", by.y = "protocol", all.x = TRUE) %>% 
      merge(.,unique(addr[,.(protocol,protocol_type = protocol_type)]), 
            by.x = "protocol", by.y = "protocol", all.x = TRUE) 
    
    p1 <- data %>% .[protocol_type=="lending"] %>% 
      ggplot(aes(x=protocol,y=ext_protocol)) +
      geom_tile(aes(fill=V1)) +
      geom_text(aes(label = round(V1)), size = 3) +
      scale_fill_gradient2(high="#756BB1",low="white", limits = c(0,100),
                           na.value="gray")+
      facet_grid(ext_protocol_type~"lending", scale="free") +
      ylab(yl) +
      theme_light() +
      theme(legend.position = "none",
            axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
            strip.text.y = element_blank(),
            axis.title.x = element_blank(),
            strip.background = element_rect(color="black", fill="#756BB1", size=1, linetype="solid"))
    
    p2 <- data %>% .[protocol_type=="dex"] %>% 
      ggplot(aes(x=protocol,y=ext_protocol)) +
      geom_tile(aes(fill=V1)) +
      geom_text(aes(label = round(V1)), size = 3) +
      scale_fill_gradient2(high="#D94801",low="white", limits = c(0,100),
                           na.value="gray")+
      facet_grid(ext_protocol_type~"dex", scale="free") +
      theme_light() +
      theme(legend.position = "none",
            axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
            axis.title.y=element_blank(),
            axis.text.y=element_blank(),
            axis.ticks.y=element_blank(),
            axis.title.x = element_blank(),
            strip.text.y = element_blank(),
            strip.background = element_rect(color="black", fill="#D94801", size=1, linetype="solid")) 
    
    p3 <- data %>% .[protocol_type=="derivatives"] %>% 
      ggplot(aes(x=protocol,y=ext_protocol)) +
      geom_tile(aes(fill=V1)) +
      geom_text(aes(label = round(V1)), size = 3) +
      scale_fill_gradient2(high="#2171B5",low="white", limits = c(0,100),
                           na.value="gray")+
      facet_grid(ext_protocol_type~"derivatives", scale="free") +
      theme_light() +
      xlab(xl) + 
      theme(legend.position = "none",
            axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
            axis.title.y=element_blank(),
            axis.text.y=element_blank(),
            axis.ticks.y=element_blank(),
            strip.text.y = element_blank(),
            axis.title.x = element_text(vjust=-1, hjust = 1),
            strip.background = element_rect(color="black", fill="#2171B5", size=1, linetype="solid")) 
    
    p4 <- data %>% .[protocol_type=="assets"] %>% 
      ggplot(aes(x=protocol,y=ext_protocol)) +
      geom_tile(aes(fill=V1)) +
      geom_text(aes(label = round(V1)), size = 3) +
      scale_fill_gradient2(high="#238B45",low="white", limits = c(0,100),
                           na.value="gray")+
      facet_grid(ext_protocol_type~"assets", scale="free") +
      theme_light() +
      theme(legend.position = "none",
            axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
            axis.title.y=element_blank(),
            axis.text.y=element_blank(),
            axis.ticks.y=element_blank(),
            axis.title.x = element_blank(),
            strip.text.y = element_blank(),
            strip.background = element_rect(color="black", fill="#238B45", size=1, linetype="solid")) 
    
    p5 <- data %>% .[is.na(protocol_type)] %>% 
      ggplot(aes(x=protocol,y=ext_protocol)) +
      geom_tile(aes(fill=V1)) +
      geom_text(aes(label = round(V1)), size = 3) +
      scale_fill_gradient2(high="#BDBDBD",low="white", limits = c(0,100),
                           na.value="gray")+
      facet_grid(ext_protocol_type~"others", scale="free") +
      theme_light() +
      theme(legend.position = "none",
            axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
            axis.title.y=element_blank(),
            axis.text.y=element_blank(),
            axis.ticks.y=element_blank(),
            axis.title.x = element_blank()) 
    
    # https://cran.r-project.org/web/packages/egg/vignettes/Ecosystem.html
    return(egg::ggarrange(p1, p2, p3, p4, p5, nrow = 1))
  }

  plot <- heat_plot(data = heat_N_pro_byExt, 
                    xl = "protocol building blocks appearance [%]",
                    yl = "external transaction to protocol"
  )

  if(!is.null(outpath_heatmap)){
    # file.path(paste0(path_figures,"/","buildingBlock_appear.pdf"))
    ggsave(filename=outpath_heatmap, 
           plot=plot,
           width = 11, height = 7, units = "cm", dpi = 300, scale = 2)
  }
  return(plot)
}


# (unused) additional plots: reduces matrix, number of external transactions

heat_plot_diag <- function(data, xl = "ext", yl = "pro"){
  data %<>% merge(.,unique(addr[,.(protocol,ext_protocol_type = protocol_type)]), 
                  by.x = "ext_protocol", by.y = "protocol", all.x = TRUE) %>% 
    merge(.,unique(addr[,.(protocol,protocol_type = protocol_type)]), 
          by.x = "protocol", by.y = "protocol", all.x = TRUE) 
  
  p1 <- data %>% .[(protocol_type=="lending") & (ext_protocol_type=="lending")] %>% 
    ggplot(aes(x=protocol,y=ext_protocol)) +
    geom_tile(aes(fill=V1)) +
    geom_text(aes(label = round(V1)), size = 2.5) +
    scale_fill_gradient2(high="#756BB1",low="white", limits = c(0,100),
                         na.value="gray")+
    scale_x_discrete(position = "top") +
    facet_wrap(ext_protocol_type~., scale="free",strip.position="bottom") +
    ylab(yl) +
    theme_light() +
    theme(legend.position = "none",
          axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=0),
          strip.text.y = element_blank(),
          axis.title.x = element_blank(),
          axis.title.y = element_blank(),
          strip.background = element_rect(color="black", fill="#756BB1", size=1, linetype="solid"))
  
  
  p2 <- data %>% .[(protocol_type=="dex") & (ext_protocol_type=="dex")] %>% 
    ggplot(aes(x=protocol,y=ext_protocol)) +
    geom_tile(aes(fill=V1)) +
    geom_text(aes(label = round(V1)), size = 2.5) +
    scale_fill_gradient2(high="#D94801",low="white", limits = c(0,100),
                         na.value="gray")+
    facet_wrap(ext_protocol_type~., scale="free",strip.position="bottom") +
    scale_y_discrete(position = "right") +
    scale_x_discrete(position = "top") +
    theme_light() +
    theme(legend.position = "none",
          axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=0),
          axis.title.y=element_blank(),
          #axis.text.y=element_blank(),
          #axis.ticks.y=element_blank(),
          axis.title.x = element_blank(),
          #strip.text.x = element_blank(),
          #strip.text.y = element_blank(),
          strip.background = element_rect(color="black", fill="#D94801", size=1, linetype="solid")) 
  
  p3 <- data %>% .[(protocol_type=="derivatives") & (ext_protocol_type=="derivatives")] %>% 
    ggplot(aes(x=protocol,y=ext_protocol)) +
    geom_tile(aes(fill=V1)) +
    geom_text(aes(label = round(V1)), size = 2.5) +
    scale_fill_gradient2(high="#2171B5",low="white", limits = c(0,100),
                         na.value="gray")+
    facet_wrap(ext_protocol_type~., scale="free",strip.position="top") +
    theme_light() +
    xlab(xl) + 
    theme(legend.position = "none",
          axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
          axis.title.y=element_blank(),
          #axis.text.y=element_blank(),
          #axis.ticks.y=element_blank(),
          #strip.text.y = element_blank(),
          #axis.title.x = element_text(vjust=-1),
          axis.title.x = element_blank(),
          strip.background = element_rect(color="black", fill="#2171B5", size=1, linetype="solid"))  
  
  p4 <- data %>% .[(protocol_type=="assets") & (ext_protocol_type=="assets")] %>% 
    ggplot(aes(x=protocol,y=ext_protocol)) +
    geom_tile(aes(fill=V1)) +
    geom_text(aes(label = round(V1)), size = 2.5) +
    scale_fill_gradient2(high="#238B45",low="white", limits = c(0,100),
                         na.value="gray")+
    facet_wrap(ext_protocol_type~., scale="free", strip.position="top") +
    scale_y_discrete(position = "right") +
    theme_light() +
    theme(legend.position = "none",
          axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
          axis.title.y=element_blank(),
          #axis.text.y=element_blank(),
          #axis.ticks.y=element_blank(),
          axis.title.x = element_blank(),
          strip.background = element_rect(color="black", fill="#238B45", size=1, linetype="solid"))  
  
  return(egg::ggarrange(p1, p2,p3,p4, nrow = 2, bottom = "protocol appearance in internal transaction [%]", 
                        left = "external transaction to protocol"))
}

heat_plot_red <-  function(data, xl = "ext", yl = "pro"){
  
    data %<>% merge(.,unique(addr[,.(protocol,ext_protocol_type = protocol_type)]), 
                  by.x = "ext_protocol", by.y = "protocol", all.x = TRUE) %>% 
    merge(.,unique(addr[,.(protocol,protocol_type = protocol_type)]), 
          by.x = "protocol", by.y = "protocol", all.x = TRUE)  %>% 
    .[(ext_protocol_type) %in% c("dex","lending")] %>% 
    .[(protocol_type) %in% c("dex","lending")]
  
  p1 <- data %>% .[protocol_type=="lending"] %>% 
    ggplot(aes(x=protocol,y=ext_protocol)) +
    geom_tile(aes(fill=V1)) +
    geom_text(aes(label = round(V1))) +
    scale_fill_gradient2(high="#756BB1",low="white", limits = c(0,100),
                         na.value="gray")+
    facet_grid(ext_protocol_type~"lending", scale="free") +
    xlab(xl) + 
    ylab(yl) +
    theme_light() +
    theme(legend.position = "none",
          axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
          strip.text.y = element_blank(),
          axis.title.x = element_text(vjust=-1, hjust = -1),
          strip.background = element_rect(color="black", fill="#756BB1", size=1, linetype="solid"))
  
  p2 <- data %>% .[protocol_type=="dex"] %>% 
    ggplot(aes(x=protocol,y=ext_protocol)) +
    geom_tile(aes(fill=V1)) +
    geom_text(aes(label = round(V1))) +
    scale_fill_gradient2(high="#D94801",low="white", limits = c(0,100),
                         na.value="gray")+
    facet_grid(ext_protocol_type~"dex", scale="free") +
    theme_light() +
    theme(legend.position = "none",
          axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
          axis.title.y=element_blank(),
          axis.text.y=element_blank(),
          axis.ticks.y=element_blank(),
          axis.title.x = element_blank(),
          strip.text.y = element_blank(),
          strip.background = element_rect(color="black", fill="#D94801", size=1, linetype="solid")) 
  
  # https://cran.r-project.org/web/packages/egg/vignettes/Ecosystem.html
  return(egg::ggarrange(p1, p2, nrow = 1))
}

