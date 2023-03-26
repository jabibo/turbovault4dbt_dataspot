def source_split(source):
  source_name = source[0:source.find("_")] 
  source_object = source[source.find("_")+1:] 
  return source_name, source_object