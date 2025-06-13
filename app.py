from fastapi import FastAPI
import subprocess
import requests
from pydantic import BaseModel

from download import download_file
from process import process_files

import os

app = FastAPI()

@app.get("/health")
def read_root():
    return {"message": "FastAPI funcionando!"}

class InsertDataRequest(BaseModel):
    nome_id: str
    url_assinada: str

@app.post("/upload-and-process")
def upload_and_process(dados: InsertDataRequest):

    response = requests.get(dados.url_assinada, stream=True)

    if response.status_code == 200:
        with open(f'{dados.nome_id}', 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print('Download')
        message = "Download realizado com sucesso"
    else:
        print('Nao consegui baixar')
        message = "Nao consegui baixar"

    # Agora é necessário processar o arquivo baixado
    try:
        process_files(f'{dados.nome_id}')
        message = "Processamento realizado com sucesso"
    except Exception as e:
        message = f"Erro ao processar o arquivo: {e}"

    os.remove(f'{dados.nome_id}')
    # Executa o programa Python (ex: seu_programa.py)
    #resultado = subprocess.run(["python", "insert.py", dados.nome_id], capture_output=True, text=True)
    
    return {
        "stdout": message
    }

