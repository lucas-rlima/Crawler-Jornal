NOME_DO_CANDIDATO = 'Lucas Rodrigues de Lima'
EMAIL_DO_CANDIDATO = 'lima.lucasr@outlook.com'

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from time import sleep
from typing import Dict, List, Tuple
import os
from datetime import datetime, date

import requests

MAIN_FOLDER = Path(__file__).parent.parent


def request_journals(start_date, end_date):
    url = 'https://engine.procedebahia.com.br/publish/api/diaries'

    r = requests.post(url, data={"cod_entity": '50', "start_date": start_date,
                                 "end_date": end_date})
    if r.status_code == 200:
        return r.json()
    elif r.status_code == 400:
        sleep(10)
        return request_journals(start_date, end_date)
    return {}

def download_jornal(edition, path):
    url = 'http://procedebahia.com.br/irece/publicacoes/Diario%20Oficial' \
          '%20-%20PREFEITURA%20MUNICIPAL%20DE%20IRECE%20-%20Ed%20{:04d}.pdf'.format(int(edition))
    r = requests.get(url, allow_redirects=True)
    if r.status_code == 200:
        with open(path, 'wb') as writer:
            writer.write(r.content)
        return edition, path
    return edition, ''

def download_mutiple_jornals(editions, paths):
    threads = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        for edition, path in zip(editions, paths):
            threads.append(executor.submit(download_jornal, edition, path))

        results = []
        for task in as_completed(threads):
            results.append(task.result())

    results = [[r for r in results if r[0] == e][0] for e in editions]
    return [r[1] for r in results]

class JournalDownloader:
    def __init__(self):
        self.pdfs_folder = MAIN_FOLDER / 'pdfs'
        self.jsons_folder = MAIN_FOLDER / 'out'

        self.pdfs_folder.mkdir(exist_ok=True)
        self.jsons_folder.mkdir(exist_ok=True)

    def get_day_journals(self, year: int, month: int, day: int) -> List[str]:
        journalday = datetime.strptime(f"{day}/{month}/{year}", '%d/%m/%Y').date()
        assert date.today() > journalday
        request = self.parse(request_journals(journalday,journalday))
        jsonpath =  self.download_all(request)
        return jsonpath
        
            
        
    def get_month_journals(self, year: int, month: int) -> List[str]:
        # TODO: get all journals of a month, returns a list of JSON paths
        mesinicio = datetime.strptime(f"01/{month}/{year}", '%d/%m/%Y').date()
        assert date.today() > mesinicio
        dia = 31
        jsonpath = list()
        while True:
            try:
                mesfim = datetime.strptime(f"{dia}/{month}/{year}", '%d/%m/%Y').date()
                m = self.parse(request_journals(mesinicio,mesfim))
            except:
                dia -= 1
            else: 
                break
        jsonpath.append(self.download_all(m))
        return jsonpath


    def get_year_journals(self, year: int) -> List[str]:
        # TODO: get all journals of a year, returns a list of JSON paths
        anoinicio = datetime.strptime(f"01/01/{year}", '%d/%m/%Y').date()
        assert date.today() > anoinicio
        anofim = datetime.strptime(f"31/12/{year}", '%d/%m/%Y').date()
        if date.today().year == year:
            anofim = date.today()
        y = self.parse(request_journals(anoinicio,anofim))
        jsonpath = list()
        jsonpath.append(self.download_all(y))
        return jsonpath
        
    @staticmethod
    def parse(response: Dict) -> List[Tuple[str, str]]:
        # TODO: parses the response and returns a tuple list of the date and edition
        dados = list()
        for b in response['diaries']:
            informacoes = (b['edicao'], b['data'])
            dados.append(informacoes)
        return dados

    def download_all(self, editions: List[str]) -> List[str]:
        # TODO: download journals and return a list of PDF paths. download in `self.pdfs_folder` folder
        #  OBS: make the file names ordered. Example: '0.pdf', '1.pdf', ...
        count = 0
        paths = list()
        for x in editions:
            try:
                os.makedirs(f'{self.pdfs_folder}/')
            except:
                pass
            download_mutiple_jornals([x[0]], [f'{self.pdfs_folder}/{count}.pdf'])
            paths.append(self.dump_json(f'{self.pdfs_folder}/{count}.pdf', x[0], x[1] ))
            count +=1
        return paths


    def dump_json(self, pdf_path: str, edition: str, date: str) -> str:
        if pdf_path == '':
            return ''
        output_path = self.jsons_folder / f"{edition}.json"
        data = {
            'path': str(pdf_path),
            'name': str(edition),
            'date': date,
            'origin': 'Irece-BA/DOM'
        }
        with open(output_path, 'w', encoding='utf-8') as file:
            file.write(json.dumps(data,
                                  indent=4, ensure_ascii=False))
        return str(output_path)