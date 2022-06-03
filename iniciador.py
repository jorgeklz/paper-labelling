"""
This file will be deleted in the future...
"""
import pickle
import collections

rows= []
  
def Knn(Matriz, k):
  labels = tipo
  pred_label = []
  etiquetar = []
  indice = 0
  for x in labels:
      if(indice != len(labels)-1):
        if(Matriz[indice]<=0.20): #Filtro que asigan directament 0 cuando el valor de comparacion sea 0.20 o menor 
          etiquetar.append((Matriz[indice], 0))
        else:
          etiquetar.append((Matriz[indice], x))
      indice += 1
  etiquetar.sort(reverse=True)
  neighbors = etiquetar[:k]
  votes = []
  for neighbor in neighbors:
            votes.append(neighbor[1])
  counter = collections.Counter(votes) #Determina el que mas se repite
  pred_label.append(counter.most_common()[0][0])
  return pred_label

def EtiquetaEnTexto(valor):
  if valor[0]==1:
    return 'emergencia'
  else:
    return 'no_emergencia'

tipo = []
for i in range(1280):
  tipo.append(1)
for j in range(1280):
  tipo.append(0)
  
modelLSI = pickle.load(open('Modelo/ModelLSI9.model','rb'))
DiccioLSI = pickle.load(open('Modelo/DiccionarioLSI9.pickle','rb'))
MatrizSimLSI = pickle.load(open('Modelo/MatrizSimilaridadLSI9.pickle','rb'))

def modeloLSI(tweet):
  # Tweet = word_tokenize(ProcesarTweet(tweet)) #Se Preprocesa el tweet y se tokeniza el texto
  
  if len(tweet) <= 2: #Se valida que el tweet preprocesado tenga un minino de palabras
    tweet = ['vacio']  #Se le asigna el texto 'vacio' el cual sera detectado como no emergencia
  
  tweet_Dic = DiccioLSI.doc2bow(tweet) #Se estructura el tweet en función al diccionario del modelo
  sim = MatrizSimLSI[modelLSI[tweet_Dic]] #Se genera un nuevo vector que contiene el nivel de similitud del nuevo tweet con cada tweet del dataset
  
  Valor_Similitud = [] 
  for i in range(len(sim)):
    a = ('%.2f' % sim[i]) #Se redondea cada valor del vector de similitud creado para establecer un mismo formato a todos los valores
    Valor_Similitud.append(float(a))
  
  return Valor_Similitud
  
def EtiquetarModelLSI(tweet):
  Similaridad= modeloLSI(tweet)
  k_5 = Knn(Similaridad, 5)
  return k_5 


# #Código para comprobar si existe Resultados
# try:
#     Docu=pd.read_csv('data_etiquetada.csv', delimiter='|')
# except:
#   with open('data_etiquetada.csv', 'a') as f:
#     writer = csv.writer(f, delimiter='|')
#     writer.writerow(['user_id', 'status_id', 'created_at', 'screen_name', 'text',
#     'status_url','lat',
#     'long', 'place_full_name', 'class', 'institution'])
#     f.close()


# if 'ids' in globals():
#   print('Directo a captura')
# else:
#   documento=pd.read_csv('data_streaming.csv', delimiter='|')
#   global ids
#   ids=documento['status_id'].values.tolist()