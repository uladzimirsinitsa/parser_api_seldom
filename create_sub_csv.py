
import os
import csv
import json


PATH_JSON = 'data/json/'

columns = ['updated', 'itemsFound', 'methodStatus', 'paramsCheckError',
           'paramsAreValid', 'parent_fullName', 'parent_shortName',
           'parent_inn', 'parent_ogrn', 'parent_regOrgan_code',
           'parent_regOrgan_name', 'parent_date', 'parent_egruName',
           'parent_code', 'parent_name', 'company_fullName',
           'company_shortName', 'company_inn', 'company_ogrn',
           'company_regOrgan_code', 'company_regOrgan_name', 'company_date',
           'company_egrulName', 'company_status', 'company_share_pct',
           'company_hist_type', 'company_date']


def create_list(data):
    temp = []

    try:
        if data.get('companies_list') is None:
            return [{
                    'itemsFound': data['status']['itemsFound'],
                    'methodStatus': data['status']['methodStatus'],
                    'paramsCheckError': data['status']['paramsCheckError'],
                    'paramsAreValid': data['status']['paramsAreValid'],
                    'parent_fullName': data['current_company']['fullName'],
                    'parent_shortName': data['current_company']['shortName'],
                    'parent_inn': str(data['current_company']['inn']),
                    'parent_ogrn': str(data['current_company']['ogrn']),
                    'parent_regOrgan_code': data['current_company']['status']['regOrgan']['code'],
                    'parent_regOrgan_name': data['current_company']['status']['regOrgan']['name'],
                    'parent_date': data['current_company']['status']['date'],
                    'parent_egruName': data['current_company']['status']['egrulName'],
                    'parent_code': data['current_company']['status']['code'],
                    'parent_name': data['current_company']['status']['name'],
                    'company_fullName': None,
                    'company_shortName': None,
                    'company_inn': None,
                    'company_ogrn': None,
                    'company_regOrgan_code': None,
                    'company_regOrgan_name': None,
                    'company_date': None,
                    'company_egrulName': None,
                    'company_status': None,
                    'company_share_pct': None,
                    'company_hist_type': None,
                    'company_date': None
                }]
    except KeyError:
        return []
    try:
        for company in data['companies_list']:
            item = {
                    'itemsFound': data['status']['itemsFound'],
                    'methodStatus': data['status']['methodStatus'],
                    'paramsCheckError': data['status']['paramsCheckError'],
                    'paramsAreValid': data['status']['paramsAreValid'],
                    'parent_fullName': data['current_company']['fullName'],
                    'parent_shortName': data['current_company']['shortName'],
                    'parent_inn': str(data['current_company']['inn']),
                    'parent_ogrn': str(data['current_company']['ogrn']),
                    'parent_regOrgan_code': data['current_company']['status']['regOrgan']['code'],
                    'parent_regOrgan_name': data['current_company']['status']['regOrgan']['name'],
                    'parent_date': data['current_company']['status']['date'],
                    'parent_egruName': data['current_company']['status']['egrulName'],
                    'parent_code': data['current_company']['status']['code'],
                    'parent_name': data['current_company']['status']['name'],
                    'company_fullName': company['company']['fullName'],
                    'company_shortName': company['company']['shortName'],
                    'company_inn': str(company['company']['inn']),
                    'company_ogrn': str(company['company']['ogrn']),
                    'company_regOrgan_code': company['company']['status']['regOrgan']['code'],
                    'company_regOrgan_name': company['company']['status']['regOrgan']['name'],
                    'company_date': company['company']['status']['date'],
                    'company_egrulName': company['company']['status']['egrulName'],
                    'company_status': company['company']['status']['name'],
                    'company_share_pct': company['share_pct'],
                    'company_hist_type': company['hist_type'],
                    'company_date': company['date']
                }
            temp.append(item)
        return temp
    except KeyError:
        return []


def read_json_filenames():
    filenames = []
    for _, _, files in os.walk(PATH_JSON):
        filenames.extend(iter(files))
    return filenames


def main():
    filenames = read_json_filenames()
    temp_1 = []
    temp_2 = []
    temp_3 = []
    temp_4 = []
    temp_5 = []
    for name in filenames:
        index = name[-6]
        with open(f"{PATH_JSON}{name}", encoding='utf-8') as file:
            if index == '1':
                data = create_list(json.load(file))
                temp_1.extend(data)
            elif index == '2':
                data = create_list(json.load(file))
                temp_2.extend(data)
            elif index == '3':
                data = create_list(json.load(file))
                temp_3.extend(data)
            elif index == '4':
                data = create_list(json.load(file))
                temp_4.extend(data)
            elif index == '5':
                data = create_list(json.load(file))
                temp_5.extend(data)
            else:
                continue
    temps = [temp_1, temp_2, temp_3, temp_4, temp_5]
    for index, temp in enumerate(temps, 1):
        if len(temp) > 0:
            with open(f'data/final_csv/sub{index}.csv', 'w', encoding='utf-8') as file:
                wr = csv.DictWriter(file, fieldnames=columns, extrasaction='ignore')
                wr.writeheader()
                wr.writerows(temp)


if __name__ == "__main__":
    main()
