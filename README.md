## Usage
```
python3 csv_parsing.py PATH_TO_CSV ROOT_DIR VENDOR_DIR -t TAG_1, TAG_2
```

## Example
given a folder structure:

```
report_NGS
	- ARCHER
	- BRCA
		- BRCA_Assay.csv
		- S109-89667_BR20002
		- S109-89713_BR20003
		- ...
	- Focus
	- Myeloid
	- Tumor Mutation Load
```
the following cmd will upload and register all the reports under BRCA folder to Seqslab platform and set `BRCA` and `Oncomine` as tags	for each report

```
python3 csv_parsing.py ./report_NGS/BRCA/BRCA_Assay.csv ./report_NGS BRCA -t Oncomine
```



## Help Document
```
python3 csv_parsing.py -h
```