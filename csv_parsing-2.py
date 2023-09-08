import csv
import io
import json
import subprocess
from datetime import datetime
import pytz
import argparse
import json
import os
import errno
from copy import deepcopy
import datetime


class AtgxMetaData:

    def __init__(self):
        self.metadata = {
            "alternate_identifiers": [],
            "dates": [],
            "types": [],
            "privacy": "",
            "primary_publication": [],
            "contributors": [],
            "licenses": [],
            "extra_properties": [],
        }

    def set_extra_properties(self, extras):
        self.metadata['extra_properties'] = extras
        return self.metadata

    def set_dates(self, dates):
        self.metadata['dates'] = dates
        return self.metadata

    def set_types(self, types):
        self.metadata['types'] = types
        return self.metadata

    def get_dictionary(self):
        return self.metadata

    @staticmethod
    def alternate_identifier_info(id, src):
        return {
            "identifier": id,
            "identifier_source": src
        }

    @staticmethod
    def date_info(date, type_value, type_value_iri=None):
        return {
            "date": date,
            "type": AtgxMetaData.annotation(type_value, type_value_iri)
        }

    @staticmethod
    def data_type(info_value=None, info_iri=None, method_value=None, method_iri=None,
                  platform_value=None, platform_iri=None, instr_value=None, instr_iri=None):
        ret = {
            "information": AtgxMetaData.type_item(info_value, info_iri),
            "method": AtgxMetaData.type_item(method_value, method_iri),
            "platform": AtgxMetaData.type_item(platform_value, platform_iri),
            "instrument": AtgxMetaData.type_item(instr_value, instr_iri),
        }
        return AtgxMetaData.remove_none_from_dict(ret)

    @staticmethod
    def privacy(src):
        return {"privacy": src}

    @staticmethod
    def primary_publications(title, date, type_value='publication'):
        return {
            "title": title,
            "venue": "",
            "dates": [AtgxMetaData.date_info(date, type_value)],
            "authors": []
        }

    @staticmethod
    def contributors():
        return []

    @staticmethod
    def licenses():
        return []

    @staticmethod
    def extra_properties(category, values=[], category_iri=None):
        ret = {
            "category": category,
            "values": values,
            "categoryIRI": category_iri
        }
        return AtgxMetaData.remove_none_from_dict(ret)

    @staticmethod
    def remove_none_from_dict(dictionary):
        for key, value in list(dictionary.items()):
            if not value:
                del dictionary[key]
            elif isinstance(value, dict):
                AtgxMetaData.remove_none_from_dict(value)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        AtgxMetaData.remove_none_from_dict(item)

        return dictionary

    @staticmethod
    def annotation(value=None, value_iri=None):
        ret = {
            "value": value,
            "valueIRI": value_iri
        }
        return AtgxMetaData.remove_none_from_dict(ret)

    @staticmethod
    def organization(name, value=None, value_iri=None):
        return {
            "name": name,
            "role": AtgxMetaData.annotation(value, value_iri)
        }

    @staticmethod
    def person(first_name=None, middle_initial=None, last_name=None, email=None):
        ret = {
            "firstName": first_name,
            "middleInitial": middle_initial,
            "lastName": last_name,
            "email": email,
        }
        return AtgxMetaData.remove_none_from_dict(ret)

    @staticmethod
    def type_item(value, value_iri=None):
        ret = {
            "value": value,
            "value_iri": value_iri
        }
        return AtgxMetaData.remove_none_from_dict(ret)


class MetaDataParser:
    tpevgh_mapping = {
        "月份": "",
        "MP. No.": "",
        "Path. No.": "",
        "Patient": "",
        "History No.": "",
        "檢體別": "",
        "Blood 採檢日": "date",
        "採檢日": "date",
        "採血日/cfTNA抽取日": "date",
        "標本送件日": "date",
        "上機日": "date",
        "Block No.": "",
        "Tumor%": "",
        "Muation gene": "",
        "Position": "",
        "Allele Frequency": "",
        "OncomineTumorType": "",
        "Diagnosis": "",
        "簽收日": "date",
        "報告日": "date",
        "TAT": "",
        "主治醫師": "",
        "Note": "",
        "DNA (ng/uL)": "",
        "DNA Barcode no.": "",
        "cfTNA (ng/uL)": "",
        "Molecular Barcode no.": "",
        "RNA(ng/uL)": "",
        "RNA preSeq Ct": "",
        "RNA Barcode no.": "",
        "Fragment major band (bp)": "",
        "DNA qPCR result(pM)": "",
        "RNA qPCR result(pM)": "",
        "DNA 上機Library input (pM)": "",
        "RNA 上機Library input (pM)": "",
        "上機Library input (pM)": "",
        "覆核後CTC Result EpCAM+ CD45-": "",
    }

    def __init__(self, schema_path=None):
        if not schema_path:
            self.mapping = self.tpevgh_mapping
        else:
            self.mapping = self.parse_schema(schema_path)

    @staticmethod
    def parse_schema(schema_path):
        with open(schema_path, 'r', newline='') as file:
            reader = csv.reader(file)
            first_row = next(reader)
            second_row = next(reader)

        ret = {}
        for i in range(0, len(first_row)):
            ret[first_row[i]] = second_row[i]
        return ret

    def parse_metadata(self, row, header_fields):
        md = AtgxMetaData()
        extra_properties = []
        dates = []

        for i in range(0, len(row)):
            try:
                col = header_fields[i]
                value = row.get(col)
                type = self.mapping.get(col, None)

                # extra_properties
                if not type:
                    if value:
                        extra_properties.append(AtgxMetaData.extra_properties(col, value))
                # date
                if type == 'date':
                    if value:
                        dates.append(AtgxMetaData.date_info(value, col))
            except Exception as e:
                print(str(e))

            md.set_dates(dates)
            md.set_extra_properties(extra_properties)

        return md.get_dictionary()


def init_arg():
    """
        run.date <- args[1]
        ref.name <- args[2]
        input.RAPIDR <- args[3]
        input.clean.count <- args[4]
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--report_path", help="e.g. 2023 NGS Test清單.xlsx", type=str, required=True)
    parser.add_argument("--schema_path", help="e.g. 2023 NGS Test schema.xlsx", type=str)
    parser.add_argument("--report_root_path",
                        help="e.g. /data",
                        type=str)
    return parser.parse_args()


def csv_preprocess(file_path):
    # Preprocess the file to remove '^M' characters
    processed_lines = []
    with open(file_path, 'r', encoding='utf-8-sig') as file:
        for line in file:
            processed_line = line.rstrip('\r\n')  # Remove '\r\n' at the end of the line
            processed_lines.append(processed_line)

    # Create a StringIO object from the processed lines
    processed_content = '\n'.join(processed_lines)
    processed_file = io.StringIO(processed_content)

    return processed_file


def find_src_path(meta_data):
    base = '/mnt/data/tpevghseqslabapi_seqslabapi685c2storage/NGS_data/'
    pp = meta_data.get('extra_properties')[2]
    mp = meta_data.get('extra_properties')[1]
    src = f"{base}/{}"
    # /mnt/data/tpevghseqslabapi_seqslabapi685c2storage/NGS_data/Myeloid Assay/2023/M112-00003_(MY23001).pdf'
    pass
"""
def seqslab_ops():
    src = f"/{pp}_({mp})/{pp}_({mp}).*"
    id = f"{pp}_{mp}"
    cmd = 'seqslab datahub upload --src "{0}" --dst {2}/{1}/ --workspace vghtpe > {2}/{1}_tmp.json'.format(
        f"{path_prefix}/{vendor}/{src}",
        f"{id}",
        f"{vendor}")

    ret = subprocess.call(cmd, shell=True)

    print(cmd)
    print(f'subprocess for upload ret {ret}')

    with open(f'{vendor}/{id}_tmp.json', 'r') as f:
        pls = json.load(f)
        for pl in pls:
            pl['id'] = f"drs_{id}"
            pl['metadata'] = {
                "types": [],
                "extra_properties": [
                    {"category": "MP_No", "values": [mp]},
                    {"category": "Path_No", "values": [pp]},
                    {"category": "Patient_Name", "values": [patient]},
                    {"category": "Tumor_Purity", "values": [tumor_purity]},
                    {"category": "Diagnosis", "values": [diagnosis]},
                    {"category": "test_item", "values": [test_item]},
                    {"category": "Physician", "values": [physician]},
                    {"category": "Turn_Around_time", "values": [tat]},
                ],
                "dates": [
                    {"date": receive_date, "type": {"value": "Time of Collection"}},
                    {"date": sign_date, "type": {"value": "Time of Report Signed"}},
                    {"date": vendor_report_date, "type": {"value": "Time of Vendor Report"}},
                    {"date": report_date, "type": {"value": "Time of VGH Report"}},
                ],
                "alternate_identifiers": [],
                "contributors": [],
                "licenses": []
            }
            pl['tags'] = [test_item]
        # print(pl['metadata']['extra_properties'][2])
        with open(f'{vendor}/{id}_upload.json', 'w') as f:
            json.dump(pls, f, indent=4, ensure_ascii=False, )

    cmd = 'seqslab datahub register-blob dir-blob --stdin < {0} --workspace vghtpe > {1}'.format(
        f'{vendor}/{id}_upload.json',
        f'{vendor}/{id}_register.json')
    ret = subprocess.call(cmd, shell=True)

    print(cmd)
    print(f'subprocess for register drs object drs_{id} ret {ret}')
"""


def main():
    args = init_arg()
    print(args.report_path)
    print(args.schema_path)
    print(args.report_root_path)

    parser = MetaDataParser(args.schema_path)

    # Parse the CSV file
    fh = csv_preprocess(args.report_path)
    reader = csv.DictReader(fh)
    header_fields = reader.fieldnames
    for row in reader:
        # Access and process the row data
        try:
            meta_data = parser.parse_metadata(row, header_fields)
            print(meta_data)

        except Exception as e:
            print(e)
            continue

    return 0


if __name__ == "__main__":
    main()

#
# def parse_row(sample, path_prefix, vendor):
#     mp = sample.get('MP No.', '').strip()
#     pp = sample.get('Path. No.', '').strip()
#     patient = sample.get('Patient', '').strip()
#     history_no = sample.get('History No.', '').strip()
#     block_no = sample.get('Block No.', '').strip()
#     tumor_purity = sample.get('Tumor purity %', '').strip()
#     diagnosis = sample.get('Diagnosis', '').strip()
#     test_item = sample.get('檢測項目', '').strip()
#     physician = sample.get('臨床主治醫師', '').strip()
#
#     year = int(pp.split('-')[0][1:]) + 1911
#     receive_date = create_date_obj(f"{year}/{sample.get('取件', '').strip()}")
#     sign_date = create_date_obj(f"{year}/{sample.get('簽收', '').strip()}")
#     vendor_report_date = create_date_obj(f"{year}/{sample.get('廠商報告', '').strip()}")
#     report_date = create_date_obj(f"{year}/{sample.get('VGH報告', '').strip()}")
#
#     tat = sample.get('TAT', '').strip()
#
#     src = f"/{pp}_({mp})/{pp}_({mp}).*"
#     id = f"{pp}_{mp}"
#     cmd = 'seqslab datahub upload --src "{0}" --dst {2}/{1}/ --workspace vghtpe > {2}/{1}_tmp.json'.format(
#         f"{path_prefix}/{vendor}/{src}",
#         f"{id}",
#         f"{vendor}")
#
#     ret = subprocess.call(cmd, shell=True)
#
#     print(cmd)
#     print(f'subprocess for upload ret {ret}')
#
#     with open(f'{vendor}/{id}_tmp.json', 'r') as f:
#         pls = json.load(f)
#         for pl in pls:
#             pl['id'] = f"drs_{id}"
#             pl['metadata'] = {
#                 "types": [],
#                 "extra_properties": [
#                     {"category": "MP_No", "values": [mp]},
#                     {"category": "Path_No", "values": [pp]},
#                     {"category": "Patient_Name", "values": [patient]},
#                     {"category": "Tumor_Purity", "values": [tumor_purity]},
#                     {"category": "Diagnosis", "values": [diagnosis]},
#                     {"category": "test_item", "values": [test_item]},
#                     {"category": "Physician", "values": [physician]},
#                     {"category": "Turn_Around_time", "values": [tat]},
#                 ],
#                 "dates": [
#                     {"date": receive_date, "type": {"value": "Time of Collection"}},
#                     {"date": sign_date, "type": {"value": "Time of Report Signed"}},
#                     {"date": vendor_report_date, "type": {"value": "Time of Vendor Report"}},
#                     {"date": report_date, "type": {"value": "Time of VGH Report"}},
#                 ],
#                 "alternate_identifiers": [],
#                 "contributors": [],
#                 "licenses": []
#             }
#             pl['tags'] = [test_item]
#         # print(pl['metadata']['extra_properties'][2])
#         with open(f'{vendor}/{id}_upload.json', 'w') as f:
#             json.dump(pls, f, indent=4, ensure_ascii=False, )
#
#     cmd = 'seqslab datahub register-blob dir-blob --stdin < {0} --workspace vghtpe > {1}'.format(
#         f'{vendor}/{id}_upload.json',
#         f'{vendor}/{id}_register.json')
#     ret = subprocess.call(cmd, shell=True)
#
#     print(cmd)
#     print(f'subprocess for register drs object drs_{id} ret {ret}')
