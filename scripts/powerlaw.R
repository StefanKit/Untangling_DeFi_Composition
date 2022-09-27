library("poweRlaw")
library("data.table")
library("magrittr")
library("ggplot2")
library("yaml")
library("scales")
library("tikzDevice")

doc <- yaml.load_file("./config.yaml")
path_tables <- doc$tables$path_tables
path_figures <- doc$figures$path_figures
tab_file <- paste(path_tables,"/deg_dist_R.txt", sep = "")


cat("Degree distribution analysis using R poweRlaw package.",
    file = tab_file)


powerlaw_analysis <- function(filename, bootstr = TRUE) {
  
  cat(sprintf("\n\n\nDegree distribution for %s: \n\n", filename),
      file = tab_file, append = "TRUE")
  
  deg <- read.csv2(sprintf("%s/%s.csv",path_tables,filename), sep = ",") %>% data.table()
  #deg <- read.csv2(sprintf("%s/defi_ca_network_pro_degree.csv",path_tables), sep = ",") %>% data.table()
  
  deg[, counts := X0]
  deg[, num := X + 1]
  deg[, X := NULL]
  deg[, X0 := NULL]
  
  m_pl_con = conpl$new(deg$counts)
  #m_pl_dis = displ$new(deg$counts)
  
  est_con = estimate_xmin(m_pl_con)
  #est_dis = estimate_xmin(m_pl_dis)
  
  m_pl_con$setXmin(est_con)
  #m_pl_dis$setXmin(est_dis)
  
  plot.data <- plot(m_pl_con, draw = F) %>% as.data.table()
  plot.aggdata <- plot.data[,.(y=max(y)),by=.(x)]
  fit.data <- lines(m_pl_con, draw = F)
  
  
  
  cat(sprintf("hat theta  = (hat k_min, hat alpha) = (%d,%.3f).\n\n",
              est_con$xmin,est_con$pars),file = tab_file,append = "TRUE")
  
  
  if (bootstr==TRUE) {
    
    bs_p = bootstrap_p(m_pl_con, no_of_sims = 5000, threads = 8,seed = 42) 
    bs_p$p
    #plot(bs_p)
    #bs_p = bootstrap_p(m_pl_dis, no_of_sims = 50, threads = 8) #10000
    
    
    cat(sprintf("Bootsrap analysis. P-value: %.3f.\n\n",
                bs_p$p),file = tab_file,append = "TRUE")
  }
  
  
  
  
  m_exp_con = conexp$new(deg$counts)
  m_exp_con$setXmin(m_pl_con$getXmin())
  est = estimate_pars(m_exp_con)
  m_exp_con$setPars(est)
  comp = compare_distributions(m_pl_con, m_exp_con)
  cat(sprintf("Comparison with exponential. Likelihood ratio: %.3f, p-value: %.3f.\n",
              comp$test_statistic,comp$p_two_sided),file = tab_file,append = "TRUE")
  
  m_ln_con = conlnorm$new(deg$counts)
  m_ln_con$setXmin(m_pl_con$getXmin())
  est = estimate_pars(m_ln_con)
  m_ln_con$setPars(est)
  comp = compare_distributions(m_pl_con, m_ln_con)
  cat(sprintf("Comparison with lognormal. Likelihood ratio: %.3f, p-value: %.3f.\n",
              comp$test_statistic,comp$p_two_sided),file = tab_file,append = "TRUE")
  
  m_wei_con = conweibull$new(deg$counts)
  m_wei_con$setXmin(m_pl_con$getXmin())
  est = estimate_pars(m_wei_con)
  m_wei_con$setPars(est)
  comp = compare_distributions(m_pl_con, m_wei_con)
  cat(sprintf("Comparison with weibull. Likelihood ratio: %.3f, p-value: %.3f.\n",
              comp$test_statistic,comp$p_two_sided),file = tab_file,append = "TRUE")
  
  return(m_pl_con)
}

make_plot <- function(res1,res2) {
  
  #invisible(capture.output(est_con1 = estimate_xmin(res1)))
  est_con1 = estimate_xmin(res1)
  plot.data1 <- plot(res1, draw = F) %>% as.data.table()
  plot.aggdata1 <- plot.data1[,.(y=max(y)),by=.(x)]
  fit.data1 <- lines(res1, draw = F)
  
  #invisible(capture.output(est_con2 = estimate_xmin(res2)))
  est_con2 = estimate_xmin(res2)
  plot.data2 <- plot(res2, draw = F) %>% as.data.table()
  plot.aggdata2 <- plot.data2[,.(y=max(y)),by=.(x)]
  fit.data2 <- lines(res2, draw = F)
  
  plt <- ggplot() + 
    geom_point(data=plot.aggdata1,mapping = aes(x, y),color = 'navyblue',shape = 20) + #,shape = 20
    geom_line(data=fit.data1, aes(x, y), colour="red",linetype = "dashed") +
    geom_point(data=plot.aggdata2,mapping = aes(x, y),color = 'forestgreen',shape = 4) + #, shape = 4
    geom_line(data=fit.data2, aes(x, y), colour="red",linetype = "dashed") +
    labs(x="Degree", y="CCDF") + 
    theme_light() + #annotation_logticks() + 
    theme(aspect.ratio = 0.5,legend.position = "none") +
    scale_x_continuous(trans='log10') +
    #scale_x_log10(breaks = trans_breaks("log10", function(x) 10^x),
    #              labels = trans_format("log10", math_format(1e-0.x))) +
    scale_y_continuous(trans='log10') 
  
  print(plt)
  
  #ggsave(filename=sprintf("%s/ccdf_together.pdf",path_figures),plot = plt,
  #       scale = 1, height = 3.5, width = 5,dpi = 300)
  
  return(plt)
}

suppressMessages(res1 <- powerlaw_analysis("defi_ca_network_pro_degree", bootstr = FALSE))
suppressMessages(res2 <- powerlaw_analysis("defi_ca_network_degree", bootstr = FALSE))

suppressMessages(plt <- make_plot(res1,res2))