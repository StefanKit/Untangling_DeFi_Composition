#install.packages("reshape2")

library("data.table")
library("magrittr")
library("ggplot2")
library("yaml")
library("scales")
library("tikzDevice")
library("reshape2")
library('RColorBrewer')



doc <- yaml.load_file("./config.yaml")
path_tables <- doc$tables$path_tables
path_figures <- doc$figures$path_figures


hm <- read.csv2(sprintf("%s/defi_ca_network_comps_heatmap.csv",path_tables), sep = ",") %>% data.table::data.table()


hm1 <-data.table::melt(hm, id.vars = "X") %>%
  .[, variable := sapply(variable, function(s){
    n<- as.character %>%
      gsub(x=s, pattern="X",replacement = "") %>%
      strsplit(x = ., split="[.]") %>% unlist()
    paste0(n[1]," (",n[2],")")
      })]


hm1[, Protocols :=as.character(X)]
hm1[,Protocols:=sapply(Protocols, function(x) replace(x, which(x=='Unknown'), 'unknown'))]
hm1[, X :=NULL]
hm1[, Components :=variable]
hm1[, variable :=NULL]
hm1[, value :=as.integer(value)]
hm1[, number := sapply(Components, function(x){strsplit(x=x, split=' ') %>% unlist() %>% .[1] %>% as.integer()})]

plt<-ggplot(data = hm1, aes(x = reorder(Components, number), y = Protocols)) +
  geom_tile(aes(fill = value))+ theme_light() +
  #scale_fill_gradient(trans = 'log',na.value = "grey50", guide = "colourbar", breaks = c(1,1e1,1e2,1e3,1e4,1e5,1e6)) +
  #scale_fill_brewer() +
  scale_fill_gradient(name = 'Node count',high="deepskyblue4",low="white",
                       na.value="gray",trans = 'log', breaks = c(1,1e1,1e2,1e3,1e4,1e5,1e6))+
  #geom_text(aes(label=value))+
  xlab("Component ids with total node counts")+
  theme(aspect.ratio = 0.5, text = element_text(size=14),
        axis.title.y = element_text(hjust = 1),
        axis.text.x = element_text(angle = 45, hjust=1),
        legend.position = c(0.18, 0.625),
        legend.background = element_rect(fill="gray",
                                         size=0.5, linetype="solid"),
        legend.text = element_text(size=10))+
  coord_flip()
plt
print(plt)
#ggsave(filename=sprintf("%s/components_heatmap.pdf",path_figures),plot = plt,
#       scale = 1, height = 3.5, width = 6,dpi = 300)

