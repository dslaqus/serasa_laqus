# import packages
import streamlit as st
import os
import numpy as np
import pandas as pd
# text preprocessing modules
from string import punctuation

# text preprocessing modules
import re  # regular expression
import warnings
import os
import pandas as pd
from s3fs.core import S3FileSystem


# aws keys stored in ini file in same path
# refer to boto3 docs for config settings
os.environ['AWS_CONFIG_FILE'] = 'aws_config.ini'

s3 = S3FileSystem(anon=False)
key = 'DadosNotion/DadosNotion_11_1_2023.csv'
bucket = 'data-science-laqus'

df = pd.read_csv(s3.open(f'{bucket}/{key}', mode='rb')).drop(columns='Unnamed: 0')




warnings.filterwarnings("ignore")
# seeding
np.random.seed(123)
 
# load stop words




# function to clean the text
@st.cache
def run_serasa(cnpj):
    return cnpj


# Set the app title
st.title("Laqus | Consulta SERASA")
st.write(
    """
    Consulta de dados da SERASA
    """
)
# Declare a form to receive a movie's review
form = st.form(key="my_form")
input_cnpj = form.text_input(label="Insira o CNPJ")
submit = form.form_submit_button(label="Consultar")

if submit:
    # Run Serasa
    result = run_serasa(input_cnpj)
 
    # Display results of the NLP task
    st.header("Resultado")
    st.write(f"CNPJ Consultado: {result}")