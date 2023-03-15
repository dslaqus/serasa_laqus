import pandas as pd
import numpy as np
import altair as alt
import plotly.express as px
import streamlit as st
from urllib.error import URLError
import os
import sys
from s3fs.core import S3FileSystem
import warnings
import re
import plotly.graph_objects as go
import requests
import pandas as pd
import numpy as np
from datetime import datetime
import boto3
import io
import time
import re
import os
from dependencies import consulta_cnpjs, retorna_consulta


# aws keys stored in ini file in same path
# refer to boto3 docs for config settings
os.environ['AWS_CONFIG_FILE'] = 'aws_config.ini'

s3 = S3FileSystem(anon=False)
key = 'streamlit/informes_FIDCs_2023-02-14.csv'
bucket = 'data-science-laqus'

uploaded_file = st.file_uploader("Choose a file")
if uploaded_file is not None:
    dataframe = pd.read_excel(uploaded_file)
    cnpjs = dataframe['CNPJ'].values.tolist()


    df = retorna_consulta(cnpjs)
    st.write(dataframe)
