
import os
import json

from dotenv import load_dotenv

load_dotenv()


PATH_JSON = 'data/json/'


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
                temp_1.append(json.load(file))
            if index == '2':
                temp_2.append(json.load(file))
            if index == '3':
                temp_3.append(json.load(file))
            if index == '4':
                temp_4.append(json.load(file))
            if index == '5':
                temp_5.append(json.load(file))
            else:
                continue

    temps = [temp_1, temp_2, temp_3, temp_4, temp_5]
    
    for index, temp in enumerate(temps, 1):
        if len(temp) > 0:
            with open(f'data/final_json/sub{index}.json', 'w', encoding='utf-8') as file:
                json.dump(temp, file, indent=4, ensure_ascii=False)


if __name__ == '__main__':
    main()
