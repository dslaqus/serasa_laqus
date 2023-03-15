import requests
import pandas as pd
from datetime import datetime
from s3fs.core import S3FileSystem
import boto3
import re
import os



def consulta_cnpjs(cnpj):

    os.environ['AWS_CONFIG_FILE'] = 'aws_config.ini'
    s3 = S3FileSystem(anon=False)



    # API endpoint URL and parameters (if applicable)
    url_base = "https://sitenet43-2.serasa.com.br/Prod/consultahttps?p=78319591Laqus@23        "
    tp_idt_cns = "J"
    tp_consulta = "    "
    parametros = f"B49C      0{cnpj}{tp_idt_cns}C     FI0000000             00SINIA                                  00000000000 00      000000000000000                     000000000000000 000000000               00                                                                                                0000000000000000                0                                   00                                         P002RE02                     REH3                     {tp_consulta}                                                         N00100PPBPCBN0N                                   S                                                                N00300000000000000000000000  NRC5                                                                                  T999"
    api_url = url_base+parametros

    # AWS S3 bucket name and directory
    bucket_name = 'data-science-laqus'
    directory = 'serasa/'


    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    # List all objects in the S3 directory
    paginator = s3_client.get_paginator('list_objects_v2')
    result_iterator = paginator.paginate(Bucket=bucket_name, Prefix=directory)
    filenames = []
    dates = []
    for result in result_iterator:
        if 'Contents' in result:
            # Extract filenames and dates from object keys
            for obj in result['Contents']:
                key = obj['Key']
                filename = os.path.basename(key)
                # Extract date from filename using regex
                match = re.search('\d{4}-\d{2}-\d{2} T \d{2}:\d{2}:\d{2}', filename)
                if match is None:
                    # Skip file if filename doesn't match expected format
                    continue
                date_str = match.group()
                date = datetime.strptime(date_str, '%Y-%m-%d T %H:%M:%S')
                if cnpj in filename:
                    filenames.append(filename)
                    dates.append(date)

    # Find the latest file for the specified document
    latest_file = None
    latest_date = datetime.min
    for filename, date in zip(filenames, dates):
        if date > latest_date:
            latest_file = directory+filename
            latest_date = date
    if latest_file is not None:
        print('Já existe no S3','   Date:',latest_date,'   File:',latest_file)
        # File exists, read contents and save to response
        s3_object = s3_client.get_object(Bucket=bucket_name, Key=latest_file)
        response = s3_object['Body'].read().decode('utf-8')
    else:
        # File does not exist, make API request and save response
        print('Não existia no S3, portanto foi feito um request')
    #         api_response = requests.post(api_url).content.decode('utf-8')
        response = requests.post(api_url).content.decode('utf-8')
        filename_ = f"{cnpj}_{datetime.now().strftime('%Y-%m-%d T %H:%M:%S')}.txt"
        print(filename_)
        s3_client.put_object(Bucket=bucket_name, Key=directory+filename_, Body=response)
        print(f'Salvo no S3|  CNPJ:{cnpj}, bucket:{bucket_name},  Key{directory+filename_}')
    #         s3_client.put_object(Bucket=bucket_name, Key=directory+filename_, Body=response.encode('utf-8'))
    return response



def retorna_consulta(cnpjs):
    # Cria o layout do dataframe final
    layout = {
        "CNPJ": [],
        "f_nome_razao_social": [],
        "f_dt_nasc_fundacao": [],
        "f_situacao_doc": [],
        "f_documentos_roubados_dados": [],
        "f_pendencias_internas_detalhe": [],
        "f_pendencias_internas_total_ocorrencias": [],
        "f_pendencias_financeiras_detalhe": [],
        "f_pendencias_financeiras_complemento": [],
        "f_pendencias_financeiras_total_ocorrencias": [],
        "f_protestos_detalhe": [],
        "f_protestos_complemento": [],
        "f_protestos_total_ocorrencias": [],
        "f_cheques_sem_fundo_bacen_detalhe": [],
        "f_cheques_sem_fundo_bacen_total_ocorrencias": [],
        "f_cnae_principal": [],
        "f_cnaes_secundarios": [],
        "f_cnae_nm": [],
        "f_score_vlr": [],
        "f_prob_inad": [],
        "f_full_string": []
    }

    df_serasa = pd.DataFrame(layout)

    index = 0


    for cnpj in cnpjs:


        # Features names
        f_nome_razao_social =""
        f_dt_nasc_fundacao =""
        f_situacao_doc =""
        f_documentos_roubados_dados =""
        f_pendencias_internas_detalhe =""
        f_pendencias_internas_total_ocorrencias =""
        f_pendencias_financeiras_detalhe =""
        f_pendencias_financeiras_complemento =""
        f_pendencias_financeiras_total_ocorrencias =""
        f_protestos_detalhe =""
        f_protestos_complemento =""
        f_protestos_total_ocorrencias =""
        f_cheques_sem_fundo_bacen_detalhe =""
        f_cheques_sem_fundo_bacen_total_ocorrencias =""
        f_cnae_principal =""
        f_cnaes_secundarios =""
        f_cnae_nm =""
        f_score_vlr =""
        f_prob_inad =""
        f_full_string = ""

        resposta = consulta_cnpjs(cnpj)

        # Segmenta e cria os blocos
        header = resposta[:400]
        values = resposta[400:]
        qtd_blocos = int((len(values)-1)/115)
        incremento = 400
        blocos = dict()
        for b in range(1,qtd_blocos+1):
            resposta[incremento:incremento+115]
            blocos[b] = resposta[incremento:incremento+115]
            incremento+=115
        blocos


        # Cria os campos
        for i in blocos:
            registro = blocos[i][:4]
            subtipo = blocos[i][4:6]
            # Confirmei
            is_confirmei = True if registro == "N200" else False
            nome_razao_social = blocos[i][6:76] if is_confirmei and subtipo == "00" else ""
            dt_nasc_fundacao = blocos[i][76:84] if is_confirmei and subtipo == "00" else ""
            situacao_doc = blocos[i][84:86] if is_confirmei and subtipo == "00" else ""
            full_string = blocos[i] if registro=="N200" else ""

            # Documentos roubados
            is_docs_roubados = True if registro=="N210" else False
            documentos_roubados_dados = blocos[i][6:] if is_docs_roubados and subtipo != "99" else ""
            full_string = blocos[i] if registro=="N210" else ""

            # Pendências internas
            is_pendencias_internas = True if registro=="N230" else False
            pendencias_internas_detalhe = blocos[i][6:] if is_pendencias_internas and subtipo == "00" else ""
            pendencias_internas_total_ocorrencias = blocos[i][6:11] if is_pendencias_internas and subtipo == "90" else ""
            full_string = blocos[i] if registro=="N230" else ""

            # Pendências financeiras
            is_pendencias_financeiras = True if registro=="N240" else False
            pendencias_financeiras_detalhe = blocos[i][6:] if is_pendencias_financeiras and subtipo == "00" else ""
            pendencias_financeiras_complemento = blocos[i][6:] if is_pendencias_financeiras and subtipo == "01" else ""
            pendencias_financeiras_total_ocorrencias = blocos[i][6:11] if is_pendencias_financeiras and subtipo == "90" else ""
            full_string = blocos[i] if registro=="N240" else ""

            # Protestos
            is_protestos = True if registro=="N250" else False
            protestos_detalhe = blocos[i][6:] if is_protestos and subtipo == "00" else ""
            protestos_complemento = blocos[i][6:] if is_protestos and subtipo == "01" else ""
            protestos_total_ocorrencias = blocos[i][6:11] if is_protestos and subtipo == "90" else ""
            full_string = blocos[i] if registro=="N250" else ""

            # Chequs sem fundo no BACEN
            is_cheque_sem_fundo_bacen = True if registro=="N270" else False
            cheques_sem_fundo_bacen_detalhe = blocos[i][6:] if is_cheque_sem_fundo_bacen and subtipo == "00" else ""
            cheques_sem_fundo_bacen_total_ocorrencias = blocos[i][6:11] if is_cheque_sem_fundo_bacen and subtipo == "90" else ""
            full_string = blocos[i] if registro=="N270" else ""

            # CNAE
            is_cnae = True if registro=="F900" else False
            is_cnae = True if is_cnae and blocos[i][4:8] == "SSCN" else False
            is_cnae = True if is_cnae and blocos[i][9:12] == "F03" else False
            cnae_principal = blocos[i][16:23]+" | " if is_cnae and blocos[i][12:13] == "P" else ""
            cnaes_secundarios = blocos[i][16:23]+" | " if is_cnae and blocos[i][12:13] == "S" else ""
            cnae_nm = blocos[i][8:] if is_cnae else ""
            full_string = blocos[i] if is_cnae else ""

            # SCORE
            is_score = True if registro=="F900" else False
            is_score = True if is_score and blocos[i][4:8] == "REH3" else False
            score_vlr = blocos[i][27:31] if is_score else ""
            prob_inad = blocos[i][69:] if is_score else ""
            full_string = blocos[i] if is_score else ""


            f_nome_razao_social += nome_razao_social.strip()
            f_dt_nasc_fundacao += dt_nasc_fundacao.strip()
            f_situacao_doc += situacao_doc.strip()
            f_documentos_roubados_dados += documentos_roubados_dados.strip()
            f_pendencias_internas_detalhe += pendencias_internas_detalhe.strip()
            f_pendencias_internas_total_ocorrencias += pendencias_internas_total_ocorrencias.strip()
            f_pendencias_financeiras_detalhe += pendencias_financeiras_detalhe.strip()
            f_pendencias_financeiras_complemento += pendencias_financeiras_complemento.strip()
            f_pendencias_financeiras_total_ocorrencias += pendencias_financeiras_total_ocorrencias.strip()
            f_protestos_detalhe += protestos_detalhe.strip()
            f_protestos_complemento += protestos_complemento.strip()
            f_protestos_total_ocorrencias += protestos_total_ocorrencias.strip()
            f_cheques_sem_fundo_bacen_detalhe += cheques_sem_fundo_bacen_detalhe.strip()
            f_cheques_sem_fundo_bacen_total_ocorrencias += cheques_sem_fundo_bacen_total_ocorrencias.strip()
            f_cnae_principal += cnae_principal.strip()
            f_cnaes_secundarios += cnaes_secundarios.strip()
            f_cnae_nm += cnae_nm.strip()
            f_score_vlr += score_vlr.strip()
            f_prob_inad += prob_inad.strip()
            f_full_string += full_string.strip()

        dicionario_features = {
            "CNPJ": cnpj,
            "f_nome_razao_social": f_nome_razao_social,
            "f_dt_nasc_fundacao": f_dt_nasc_fundacao,
            "f_situacao_doc": f_situacao_doc,
            "f_documentos_roubados_dados": f_documentos_roubados_dados,
            "f_pendencias_internas_detalhe": f_pendencias_internas_detalhe,
            "f_pendencias_internas_total_ocorrencias": f_pendencias_internas_total_ocorrencias,
            "f_pendencias_financeiras_detalhe": f_pendencias_financeiras_detalhe,
            "f_pendencias_financeiras_complemento": f_pendencias_financeiras_complemento,
            "f_pendencias_financeiras_total_ocorrencias": f_pendencias_financeiras_total_ocorrencias,
            "f_protestos_detalhe": f_protestos_detalhe,
            "f_protestos_complemento": f_protestos_complemento,
            "f_protestos_total_ocorrencias": f_protestos_total_ocorrencias,
            "f_cheques_sem_fundo_bacen_detalhe": f_cheques_sem_fundo_bacen_detalhe,
            "f_cheques_sem_fundo_bacen_total_ocorrencias": f_cheques_sem_fundo_bacen_total_ocorrencias,
            "f_cnae_principal": f_cnae_principal,
            "f_cnaes_secundarios": f_cnaes_secundarios,
            "f_cnae_nm": f_cnae_nm,
            "f_score_vlr": f_score_vlr,
            "f_prob_inad": f_prob_inad,
            "f_full_string":values
        }

        df_serasa_run = pd.DataFrame(dicionario_features, index=[index])
        df_serasa = pd.concat([df_serasa,df_serasa_run])
        index+=1
    return df_serasa