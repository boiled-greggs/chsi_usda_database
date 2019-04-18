import xlrd
import csv

def csv_from_excel():

    wb = xlrd.open_workbook('Education.xls')
    sh = wb.sheet_by_name('Education\ 1970\ to\ 2016')
    your_csv_file = open('education.csv', 'wb')
    wr = csv.writer(your_csv_file, quoting=csv.QUOTE_ALL)

    for rownum in xrange(sh.nrows):
        wr.writerow(sh.row_values(rownum))

    your_csv_file.close()


