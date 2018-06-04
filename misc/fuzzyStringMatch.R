
fuzzyStringMatch <- function(missing, 
                             rest, 
                             distance.methods= c('osa','lv','dl','hamming','lcs','qgram','cosine','jaccard','jw'),
                             parallel = T,
                             ncpus = 6){
  
  
 
  # registerDoMC(ncpus)
  # from https://www.r-bloggers.com/fuzzy-string-matching-a-survival-skill-to-tackle-unstructured-information/
  library(stringdist)
  library(iterators)
   
  dist.methods <- foreach(m = distance.methods, .combine=append,.packages  = c("stringdist","iterators"), .inorder = T) %do% {
    dist.name.enh  <- stringdistmatrix(missing, rest, method = m)
    list(dist.name.enh)
  }
  
  names(dist.methods) <- distance.methods
  
  if(parallel){
    library(parallel)
    library(doSNOW)
    cl <- makeCluster(ncpus, type="SOCK")
    registerDoSNOW(cl)
    match.s1.s2.enh <- foreach(el = 1:length(dist.methods),.combine=rbind,.packages =  c("stringdist","foreach","iterators","dplyr"),.inorder = T) %dopar% {
      dist.matrix<-as.matrix(dist.methods[[el]])
      itx <- iter(dist.matrix,by="row")
      foreach(row = itx,.combine=rbind,.packages  = c("stringdist","iterators"),.export=c("rest")) %do% {
        ind <- which.min(row)
        data.frame(matched = rest[ind], adist = row[ind])
      } %>% cbind(missing=missing,method=distance.methods[el])
    }
    stopCluster(cl)
    
  } else {
    match.s1.s2.enh <- foreach(el = 1:length(dist.methods),.combine=rbind,.packages =  c("stringdist","foreach","iterators","dplyr"),.inorder = T) %do% {
      dist.matrix<-as.matrix(dist.methods[[el]])
      itx <- iter(dist.matrix,by="row")
      foreach(row = itx,.combine=rbind,.packages  = c("stringdist","iterators"),.export=c("rest")) %do% {
        ind <- which.min(row)
        data.frame(matched = rest[ind], adist = row[ind])
      } %>% cbind(missing=missing,method=distance.methods[el])
    }
  }
  
  matched.names.matrix<-dcast(match.s1.s2.enh,missing+matched~method, value.var = "adist") 
  matched.names.matrix$valid.dist <- apply(matched.names.matrix[,distance.methods],1,function(x) sum(!is.na(x) & x!=Inf))
  return(matched.names.matrix)  
}
