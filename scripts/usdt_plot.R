library("data.table")
library("ggplot2")
library("magrittr")
library("yaml")


# import config file
con <- file("./config.yaml", "r"); config <- read_yaml(con); close(con);

# create pathes
path_building_block_data = config$files$path_building_block_data

plot_usdt_protocol_bar <- function(
    fn_token_dep = "0xdAC1-11565019-12964999_Protocol_Depenency.csv.gz",
    output_plot = NULL){
  
  path_token_dep <- paste0(path_building_block_data,'/case_study/',fn_token_dep)
  
  dt_token <- read.csv2(path_token_dep, sep = ",", dec = ".") %>% 
    as.data.table()
  dt_token_long <- dt_token %>% melt.data.table(data = .,id.vars =  "to_protocol",
                                              measure.vars = c("TOKEN_perDir", "TOKEN_perIn")) 
  dt_token_long[, value := value * 100]
  
  p <- dt_token_long %>% 
    ggplot() +
    geom_bar(aes(x = to_protocol, y = value, fill = variable), stat = 'identity', position = "dodge2") +
    theme_light() +
    ggtitle( "") +
    xlab("DeFi protocols")+
    ylab("Building Blocks containing USDT [%]")+
    theme(legend.position = 'right',
          axis.text.x = element_text(angle = 45, vjust = 1, hjust=1),
          axis.title.x = element_text(vjust = -0.5)) +
    scale_fill_manual(values = c("TOKEN_perDir"="black",
                                 "TOKEN_perIn"="gray"),
                      labels = c("TOKEN_perDir"="directly",
                                 "TOKEN_perIn"="indirect")) +
    guides(fill = guide_legend(title = "USDT included", 
                               title.position = "top", ncol = 1))
  
    if(!is.null(output_plot)){
      # "./outfiles/figures/protocol_bb_usdt_dependency.pdf"
      p %>% ggsave(filename = output_plot,
                   width = 20, height = 10, units = "cm")
    }
  
    return(p)
}

