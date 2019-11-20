#!/bin/sh
# --excelfile emeadevit.xlsx --sheetname Q3 --minrow 33 --maxrow 38 --mincol 30 --maxcol 32
python excelreader.py --excelfile emeadevit.xlsx --sheetname Q3 --collection Q3 --drop --mincol 30 --maxcol 32 --minrow 33 --maxrow 38 --host ${MDBHOST}
python excelreader.py --excelfile emeadevit.xlsx --sheetname Q4 --collection Q4 --drop --mincol 20 --maxcol 22 --minrow 32 --maxrow 37 --host ${MDBHOST}
python excelreader.py --excelfile emeadevit.xlsx --sheetname Q6 --collection Q6 --drop --mincol 21 --maxcol 23 --minrow 29 --maxrow 31 --host ${MDBHOST}
python excelreader.py --excelfile age-breakdown.xlsx --sheetname Q5 --collection Q5 --drop --mincol 1 --maxcol 6 --minrow 7 --maxrow 12  --host ${MDBHOST}
