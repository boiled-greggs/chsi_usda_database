import psycopg2
import psycopg2.extras
import csv

import pandas as pd
import math



class Database():
    """
    Class that holds connection to a database.  Three arguments on initialization are used to log into the database:
    dbname: str of database name
    user:   str of user name
    pasw:   str of password

    conn:   connection to the database.
    """
    def __init__(self, dbname, user, pasw):
        self.connection_string = "host='localhost' dbname='{:}' user='{:}' password='{:}'".format(dbname, user, pasw)
        self.set_up_file = "create_database.sql"
        self.conn = psycopg2.connect(self.connection_string)

    """
    Set up the database as specified in the setup file 'create_database.sql' (Second deliverable).
    """
    def set_up(self):
        with self.conn.cursor() as cursor:
            with open(self.set_up_file, 'r') as setup:
                setup_queries = setup.read()
                cursor.execute(setup_queries)
            self.conn.commit()

    def to_int(self, n):
        try:
            return int(n)
        except ValueError:
            try:
                m = float(n)
                if math.isnan(m):
                    return None
                return m
            except ValueError:
                return None

    def set_index(self, fname):
        with open(fname, 'r', encoding="iso-8859-1") as i_csv:
            i_csv.readline()
            u = csv.reader(i_csv, delimiter=',', quotechar='"')
            for l in u:
                for t in [0, 3]:
                    l[t] = l[t].replace(',', '')
                    l[t] = self.to_int(l[t])
                if l[0] is None:
                    continue
                self.insert("index", [l[0],l[1],l[2],l[3]])
        self.conn.cursor().execute("UPDATE index \
                                           SET state = %s \
                                           WHERE fips = %s;", ('VT', 50000))
        self.conn.commit()

    def unemployment(self, fname):
        with open(fname, 'r', encoding="iso-8859-1") as un_csv:
            un_csv.readline()
            u = csv.reader(un_csv, delimiter=',', quotechar='"')
            for l in u:
                for t in [0,18,19,20,21,46,47,48,49]:
                    l[t] = l[t].replace(',', '')
                    l[t] = self.to_int(l[t])
                if l[0] is None or l[0] in [2201, 2232, 2280]:
                    continue
                self.insert("unemployment", [l[0],2010,l[18],l[19],l[20],l[21]])
                self.insert("unemployment", [l[0],2017,l[46],l[47],l[48],l[49]])
        self.conn.commit()

    def poverty17(self, fname):
        with open(fname, 'r', encoding="iso-8859-1") as pv_csv:
            pv_csv.readline()
            u = csv.reader(pv_csv, delimiter=',', quotechar='"')
            for l in u:
                for t in [0, 7, 10, 13, 16, 19, 22, 25]:
                    l[t] = l[t].replace(',', '')
                    l[t] = self.to_int(l[t])
                if l[0] is None:
                    continue
                self.insert("poverty", [l[0],2017,l[7],l[10],l[13],l[16],l[19],l[22],l[25]])
        self.conn.commit()


    def povertyU17(self, fname):
        with open(fname, 'r',encoding="iso-8859-1") as pv_csv:
            pv_csv.readline()
            u = csv.reader(pv_csv, delimiter=',', quotechar='"')
            for l in u:
                for t in [0, 118, 108]:
                    l[t] = l[t].replace(',', '')
                    l[t] = self.to_int(l[t])
                if l[0] is None:
                    continue
                self.conn.cursor().execute("UPDATE poverty \
                                           SET singleparentpercent = %s, ginicoefficient = %s \
                                           WHERE fips = %s and year = 2017", (l[118], l[108], l[0]))
        self.conn.commit()

    def population(self, fname):
        with open(fname, 'r', encoding="iso-8859-1") as pop_csv:
            pop_csv.readline()
            u = csv.reader(pop_csv, delimiter=',', quotechar='"')
            for l in u:
                for t in [0, 8, 10, 27, 35, 51, 59, 17, 33, 41, 57, 65]:
                    l[t] = l[t].replace(',', '')
                    l[t] = self.to_int(l[t])
                if l[0] is None:
                    continue
                self.insert("population", [l[0],2010,l[8],l[10],l[27],l[35],l[51],l[59]])
                self.insert("population", [l[0],2017,None,l[17],l[33],l[41],l[57],l[65]])
        self.conn.commit()

    def education10(self, fname):
        with open(fname, 'r',encoding="iso-8859-1") as ed_csv:
            ed_csv.readline()
            u = csv.reader(ed_csv, delimiter=',', quotechar='"')
            for l in u:
                for t in [2, 90, 89, 88]:
                    l[t] = l[t].replace(',','')
                    l[t] = self.to_int(l[t])
                if l[2] is None or l[2] in [2201, 2232, 2280, 2270]:
                    continue
                if l[2] in [46113, 51515]:
                    self.insert("index", [l[2], l[3], l[4], None])
                self.insert("education", [l[2],2010,l[90],l[89],l[88]])
        self.conn.commit()

    def education17(self, fname):
        with open(fname, 'r',encoding="iso-8859-1") as ed_csv:
            ed_csv.readline()
            u = csv.reader(ed_csv, delimiter=',', quotechar='"')
            for l in u:
                for t in [0, 116, 115, 114]:
                    l[t] = l[t].replace(',','')
                    l[t] = self.to_int(l[t])
                if l[0] is None or l[0] in [2201, 2232, 2280, 2270]:
                    continue
                self.insert("education", [l[0],2017,l[116],l[115],l[114]])
        self.conn.commit()

    def health10(self, fname):
        measures = pd.read_excel(fname, sheet_name=3, header=None)
        for index, l in measures.iterrows():
            if index < 2:
                continue
            health = []
            for t in [0, 6, 11, 16, 21, 27, 32, 36, 41, 52, 55, 59, 68, 71, 76, 113, None, 131]:
                if t is None:
                    health.append(None)
                else:
                    health.append(self.to_int(l[t]))
            if health[0] is None or health[0] in [2201, 2232, 2280, 2270]:
                continue
            health.insert(1,2010)

            quartile = []
            for t in [0, 9, 14, 19, 24, 30, 35, 39, 44, 53, 58, 62, 69, 74, 79, 114, 121, 132]:
                quartile.append(self.to_int(l[t]))
            quartile.insert(1,2010)
            self.insert("health", health)
            self.insert("healthquartile", quartile)
        self.conn.commit()

    def health17(self, fname):
        measures = pd.read_excel(fname, sheet_name=3, header=None)
        for index, l in measures.iterrows():
            if index < 2:
                continue
            health = []
            for t in [0, 4, 8, 12, 16, 23, 27, 31, 43, 54, 58, 63, 69, 80, 85, 128, None, None]:
                if t is None:
                    health.append(None)
                else:
                    health.append(self.to_int(l[t]))
            if health[0] is None or health[0] in [2201, 2232, 2280, 2270]:
                continue
            health.insert(1, 2017)

            quartile = []
            for t in [0, 7, 11, 15, 19, 26, 30, 34, 46, 55, 61, 66, 70, 83, 88, 129, 136, None]:
                if t is None:
                    quartile.append(None)
                else:
                    quartile.append(self.to_int(l[t]))
            quartile.insert(1,2017)

            self.insert("health", health)
            self.insert("healthquartile", quartile)
        self.conn.commit()

    def healthU10(self, fname):
        with open(fname, 'r',encoding="iso-8859-1") as h_csv:
            h_csv.readline()
            u = csv.reader(h_csv, delimiter=',', quotechar='"')
            for l in u:
                for t in [0, 133]:
                    l[t] = l[t].replace(',','')
                    l[t] = self.to_int(l[t])
                if l[0] is None or l[0] in [2201, 2232, 2280, 2270]:
                    continue
                self.conn.cursor().execute("UPDATE health \
                                           SET pollution = %s \
                                           WHERE fips = %s and year = 2010", (l[133], l[0]))
        self.conn.commit()

    def healthU17(self, fname):
        with open(fname, 'r',encoding="iso-8859-1") as h_csv:
            h_csv.readline()
            u = csv.reader(h_csv, delimiter=',', quotechar='"')
            for l in u:
                for t in [0, 159]:
                    l[t] = l[t].replace(',','')
                    l[t] = self.to_int(l[t])
                if l[0] is None or l[0] in [2201, 2232, 2280, 2270]:
                    continue
                self.conn.cursor().execute("UPDATE health \
                                           SET pollution = %s \
                                           WHERE fips = %s and year = 2017", (l[159], l[0]))
        self.conn.commit()

    def rankings(self, fname, year):
        sub = pd.read_excel(fname, sheet_name=2, header=None)
        for index, l in sub.iterrows():
            if index < 2:
                continue
            subrank = []
            for t in [0, 4, 6, 8, 10, 12, 14]:
                subrank.append(self.to_int(l[t]))
            if subrank[0] is None or subrank[0] in [2201, 2232, 2280, 2270]:
                continue
            subrank.insert(1, year)

            subquartile = []
            for t in [0, 5, 7, 9, 11, 13, 15]:
                subquartile.append(self.to_int(l[t]))
            subquartile.insert(1, year)

            self.insert("subrankings", subrank)
            self.insert("subrankingsquartiles", subquartile)

        rank = pd.read_excel(fname, sheet_name=1, header=None)
        for index, l in rank.iterrows():
            if index < 2:
                continue
            r = []
            for t in [0, 4, 5, 6, 7]:
                r.append(self.to_int(l[t]))
            if r[0] is None or r[0] in [2201, 2232, 2280, 2270]:
                continue
            r.insert(1, year)

            self.insert("rankings", r)
        self.conn.commit()

    def insert(self, db, data):
        sstr = str(tuple(['%s'] * len(data))).replace("'", "")
        self.conn.cursor().execute("INSERT INTO {:} VALUES {:}".format(db, sstr), tuple(data))

if __name__ == '__main__':
    a = Database('chsi_usda_ers','chsi','chsi')
    a.set_up()

    a.set_index("PopulationEstimates.csv")

    a.population("PopulationEstimates.csv")

    a.poverty17("PovertyEstimates.csv")
    a.povertyU17("2017CHR_CSV_Analytic_Data.csv")

    a.unemployment("Unemployment.csv")

    a.education10("analytic_data2010.csv")
    a.education17("2017CHR_CSV_Analytic_Data.csv")

    a.health10("2010 County Health Rankings National Data_v2.xls")
    a.healthU10("analytic_data2010.csv")
    a.health17("2017CountyHealthRankingsData.xls")
    a.healthU17("2017CHR_CSV_Analytic_Data.csv")

    a.rankings("2010 County Health Rankings National Data_v2.xls", 2010)
    a.rankings("2017CountyHealthRankingsData.xls", 2017)

