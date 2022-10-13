import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

def magnify():
    return [dict(selector="th", props=[("font-size", "7pt")]),
            dict(selector="td", props=[('padding', "0em 0em")]),
            dict(selector="th:hover", props=[("font-size", "12pt")]),
            dict(selector="tr:hover td:hover", 
                 props=[('max-width', '200px'), ('font-size', '12pt')])
]

# FUNCTION DEFINTIONS

def HS_Simple_Comp(A,B, Table):
  '''
  Takes two rows, defined by index values (A and B) from a table with HS code columns. Provides the 
  cosine simliarity of A and B. This measures directional simliarity in cartesean vector space. 
  '''
  Table = spark.read.table(str(Table)).toPandas()
  num_df = (Table.iloc[[A,B]]).select_dtypes(include = ['int64'])
  score = cosine_similarity(num_df)[0,1]
  return score

def HS_Cosine_Matrix(Table):
  '''
  Takes a table with HS codes and produces a cosine simliarity of A and B. This measures directional
  simliarity in cartesean vector space. 
  '''
  Table = spark.read.table(str(Table)).toPandas()

  matrix = pd.DataFrame(cosine_similarity(Table.select_dtypes(include = ['int64'])))

  return matrix.style.background_gradient(cmap='coolwarm', axis=1)\
    .set_properties(**{'max-width': '80px', 'font-size': '8pt'})\
    .set_caption("Hover to magify")\
    .set_table_styles(magnify())

def hs_simple_comp_from_pdf(A,B, Table):
  '''
  Takes two rows, defined by index values (A and B) from a table with HS code columns. Provides the 
  cosine simliarity of A and B. This measures directional simliarity in cartesean vector space. 
  '''
  score = cosine_similarity(Table.iloc[[A,B]])[0,1]
  return score

def hs_cosine_matrix_from_pdf(Table):
  '''
  Takes a table with HS codes and produces a cosine simliarity of A and B. This measures directional
  simliarity in cartesean vector space. 
  
  Args:
      Table (pandas.DataFrame): the df containing the raw HS specta (not normalized). The table should 
      have the spectra for each entity in a row (tidy data)
  '''

  matrix = pd.DataFrame(cosine_similarity(Table))

  return matrix.style.background_gradient(cmap='coolwarm', axis=1)\
    .set_properties(**{'max-width': '80px', 'font-size': '8pt'})
    # .set_caption("Hover to magify")
    # .set_table_styles(magnify())