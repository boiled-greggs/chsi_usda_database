from load_data import *

'''
Takes in a Database object and gathers attribute data FROM the databaset.
This would essentially allow for new FIPs codes (new census regions or states).
'''
class Attributes():
	def __init__(self, db):
		self.connection_string = db.connection_string
		self.conn = psycopg2.connect(self.connection_string)
		self.st = [] #Utah
		self.stab = [] #UT
		self.rucLow = 10000
		self.rucHigh = -1
		self.stateAB = [] #(Utah, UT)

		if self.exists():
			self.load_members()
			

	def load_members(self):
		with self.conn.cursor() as cursor:
			# Get information FROM index
			cursor.execute("SELECT DISTINCT state FROM index;")
			for i in cursor:
				self.stab.append(i[0])
			
			cursor.execute("SELECT county FROM index WHERE fips % 1000 = 0;")
			for i in cursor:
				self.st.append(i[0])
			
			cursor.execute("SELECT DISTINCT ruralurbancont FROM index WHERE ruralurbancont IS NOT NULL;")
			for i in cursor:
				self.rucLow = min(self.rucLow, i[0])
				self.rucHigh = max(self.rucHigh, i[0]) 

			cursor.execute("SELECT county, state FROM index WHERE fips % 1000 = 0;")
			self.stateAB =  cursor.fetchall()
	'''
	Check if a given state (state) is represented as a full name ie (Utah)
	or as an abbreviation ie (UT).  Returns a tuple (n, State) WHERE n == 0
	indicates a full name, 1 indicates abbreviation, -1 indicates neither.
	State gives the corresponding name, exactly as it is in database.
	Could be extended for fuzzy string matching.
	'''
	def check_state(self, state):
		if state.lower() == "dc" or state.lower() == "washington dc":
			return ("Washington DC", "DC")
		if state.strip().lower().title() in self.st: #Utah
			return (0, state.strip().lower().title())
		elif state.strip().upper() in self.stab: #UT
			return (1, state.strip().upper())
		return (-1, state)

	'''
	Gets the (state, abreviation) FROM either the abbreviation or state name.
	'''
	def get_state(self,state):
		print(state)
		if state.lower() == "dc" or state.lower() == "washington dc":
			return ("Washington DC", "DC")
		if state.strip().lower().title() in self.st: #Utah
			for i in self.stateAB:
				if i[0] == state.strip().lower().title():
					return (state.strip().lower().title(), i[1])
		elif state.strip().upper() in self.stab: #UT
			for i in self.stateAB:
				if i[1] == state.strip().upper():
					return (i[0], state.strip().upper() )
	'''
	Return a list of FIPS codes for a given state string(full or abbrev).
	First index in the list is -1 on state not existing in index.
	'''
	def get_fips_state(self, state):
		st = self.check_state(state)
		if st[0] == -1:
			return [-1]
		with self.conn.cursor() as cursor:
			if st[0] == 1:
				cursor.execute("SELECT DISTINCT fips, county FROM index WHERE state = %s;", (st[1],))
			elif st[0] == 0:
				cursor.execute("SELECT DISTINCT fips, county FROM index, (SELECT state FROM index WHERE county = %s) u WHERE u.state = index.state;", (st[1],))
			return cursor.fetchall() 

	'''
	Print all states and their corresponding abbreviation "State (AB)"
	'''
	def print_states(self):
		for i in self.stateAB:
			print("{:} ({:})".format(i[0],i[1]))

	'''
	Returns true if the databases exist and are populated. Else returns false
	'''
	def exists(self):
		try:
			with self.conn.cursor() as cursor:
				cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s)", ('index',))
				return cursor.fetchone()[0]
		except Exception as e:
			return False



	###COMPARE TWO QUERIES------------------------------------------------------------------------------------------------------------
	#Since table name must be hardcoded - use these as template
	def ed_query(self, f1, f2):
		with self.conn.cursor() as cursor:
			cursor.execute("SELECT fips, year, highschoolenrollment, hsdiplomas, hsgraduationrate FROM education WHERE fips = %s OR fips = %s ORDER BY year, fips;",(f1,f2))
			return cursor.fetchall()

	def un_query(self,f1,f2):
		with self.conn.cursor() as cursor:
			cursor.execute("SELECT fips, year, unemployed, laborforce, unemployedpercent FROM unemployment WHERE fips = %s or fips = %s ORDER BY year, fips;",(f1,f2))
			return cursor.fetchall()

	def pop_query(self,f1,f2):
		with self.conn.cursor() as cursor:
			cursor.execute("SELECT fips, year, populationestimate, births, deaths, internationalmigrants, domesticmigrants FROM population WHERE fips = %s or fips = %s ORDER BY year, fips;",(f1,f2))
			return cursor.fetchall()

	def pov_query(self,f1,f2):
		with self.conn.cursor() as cursor:
			cursor.execute("SELECT fips, year, povertypopulation, percentpovertypop, percentpovertychildren, percentpovertyunderfive, medianhouseholdinc, singleparentpercent, ginicoefficient FROM poverty WHERE fips = %s or fips = %s ORDER BY year, fips;",(f1,f2))
			return cursor.fetchall()

	def gh_query(self,f1,f2):
		with self.conn.cursor() as cursor:
			cursor.execute("SELECT fips, year, ypll, poorhealth, poorphysicaldays, poormentaldays, pollution FROM health WHERE fips = %s or fips = %s ORDER BY year, fips;",(f1,f2))
			return cursor.fetchall()

	def sub_query(self,f1,f2):
		with self.conn.cursor() as cursor:
			cursor.execute("SELECT fips, year, adultsmokingpercent, adultobesitypercent, bingedrinkingpercent, liquorstoredensity, violentcrimerate FROM health WHERE fips = %s or fips = %s ORDER BY year, fips;",(f1,f2))
			return cursor.fetchall()

	def bh_query(self,f1,f2):
		with self.conn.cursor() as cursor:
			cursor.execute("SELECT fips, year, lowbirthweightpercent, stirate, teenbirthrate FROM health WHERE fips = %s or fips = %s ORDER BY year, fips;",(f1,f2))
			return cursor.fetchall()

	def mc_query(self,f1,f2):
		with self.conn.cursor() as cursor:
			cursor.execute("SELECT fips, year, uninsuredpercent, pcprate, preventablehospitalstays, diabeticscreening FROM health WHERE fips = %s or fips = %s ORDER BY year, fips;",(f1,f2))
			return cursor.fetchall()

	###RANKING QUERIES------------------------------------------------------------------------------------------------------------
	# NOTE: Even though format is being used, the rel is a parameter that comes from a hardcoded CATEGORIES_ list in application.py - no injection.
	# ALSO: ord is restricted to being "ASC" or "DESC", selected by the user providing an int - no injection worries.
	def rank_query(self,f1, ord, rel):
		with self.conn.cursor() as cursor:
			cursor.execute("SELECT county FROM index, {:} WHERE state = %s AND index.fips = {:}.fips AND year = 2010 ORDER BY {:}.{:};".format(rel, rel, rel, ord), (f1,))
			d10 = cursor.fetchall()	#SQL Injection is not a worry here, rel and ord are hard coded in application.py
			cursor.execute("SELECT county FROM index, {:} WHERE state = %s AND index.fips = {:}.fips AND year = 2017 ORDER BY {:}.{:};".format(rel, rel, rel, ord), (f1,))
			d17 = cursor.fetchall() 
			out = []
			for i in range(min(len(d10),len(d17))):
				out.append([d10[i][0],d17[i][0]])
			if min(len(d10),len(d17)) == len(d10):
				for i in range(len(d10),len(d17)):
					out.append([None, d17[i][0]])
			else:
				for i in range(len(d17),len(d10)):
					out.append([d10[i][0],None])
			return out

	###HEATMAP QUERIES------------------------------------------------------------------------------------------------------------
	def demo_heat_query(self, state):
		with self.conn.cursor() as cursor:
			if state == "ALL":
				cursor.execute("SELECT hsgraduationrate, unemployedpercent, populationestimate, CAST(births AS FLOAT)/ CAST(populationestimate AS FLOAT) birthrate,\
			 				CAST(deaths AS FLOAT)/CAST(populationestimate AS FLOAT) deathrate, percentpovertypop, percentpovertyunderfive, medianhouseholdinc, ruralurbancont FROM education, unemployment, \
			 				population, poverty, index WHERE index.fips = population.fips AND index.fips = unemployment.fips AND index.fips = education.fips AND index.fips = poverty.fips AND \
			 				education.year = 2017 AND unemployment.year = 2017 AND population.year = 2017 AND poverty.year = 2017 and index.fips % 1000 != 0 ORDER BY index.fips;")
			else:
				cursor.execute("SELECT hsgraduationrate, unemployedpercent, populationestimate, CAST(births AS FLOAT)/ CAST(populationestimate AS FLOAT) birthrate,\
			 				CAST(deaths AS FLOAT)/CAST(populationestimate AS FLOAT) deathrate, percentpovertypop, percentpovertyunderfive, medianhouseholdinc, ruralurbancont FROM education, unemployment, \
			 				population, poverty, index WHERE index.fips = population.fips AND index.fips = unemployment.fips AND index.fips = education.fips AND index.fips = poverty.fips AND \
			 				index.state = %s AND education.year = 2017 AND unemployment.year = 2017 AND population.year = 2017 AND poverty.year = 2017 AND MOD(index.fips,1000) != 0 ORDER BY index.fips;", (state,))
			return cursor.fetchall()

	def health_heat_query(self, state):
		with self.conn.cursor() as cursor:
			if state == "ALL":
				cursor.execute("SELECT ypll, poorhealth, pollution, adultsmokingpercent, adultobesitypercent, bingedrinkingpercent, lowbirthweightpercent, stirate, teenbirthrate, uninsuredpercent \
								FROM health, index WHERE index.fips = health.fips;")
			else:
				cursor.execute("SELECT ypll, poorhealth, pollution, adultsmokingpercent, adultobesitypercent, bingedrinkingpercent, lowbirthweightpercent, stirate, teenbirthrate, uninsuredpercent\
								FROM health, index WHERE index.fips = health.fips AND index.state = %s;", (state,))
			return cursor.fetchall()

	def both_heat_query(self, state):
		with self.conn.cursor() as cursor:
			if state == "ALL":
				cursor.execute("SELECT hsgraduationrate, unemployedpercent, populationestimate, CAST(births AS FLOAT)/ CAST(populationestimate AS FLOAT) birthrate,\
			 				CAST(deaths AS FLOAT)/CAST(populationestimate AS FLOAT) deathrate, percentpovertypop, percentpovertyunderfive, medianhouseholdinc, ruralurbancont, \
			 				ypll, poorhealth, pollution, adultsmokingpercent, adultobesitypercent, bingedrinkingpercent, lowbirthweightpercent, stirate, \
			 				teenbirthrate, uninsuredpercent FROM education, unemployment, population, poverty, health, index \
			 				WHERE index.fips = population.fips AND index.fips = unemployment.fips AND index.fips = education.fips AND index.fips = poverty.fips AND index.fips = health.fips AND \
			 				education.year = 2017 AND unemployment.year = 2017 AND population.year = 2017 AND poverty.year = 2017 AND health.year = 2017 AND index.fips % 1000 != 0 ORDER BY index.fips;")
			else:
				cursor.execute("SELECT hsgraduationrate, unemployedpercent, populationestimate, CAST(births AS FLOAT)/ CAST(populationestimate AS FLOAT) birthrate,\
			 				CAST(deaths AS FLOAT)/CAST(populationestimate AS FLOAT) deathrate, percentpovertypop, percentpovertyunderfive, medianhouseholdinc, ruralurbancont, \
			 				ypll, poorhealth, pollution, adultsmokingpercent, adultobesitypercent, bingedrinkingpercent, lowbirthweightpercent, stirate, \
			 				teenbirthrate, uninsuredpercent FROM education, unemployment, population, poverty, health, index \
			 				WHERE index.fips = population.fips AND index.fips = unemployment.fips AND index.fips = education.fips AND index.fips = poverty.fips AND index.fips = health.fips AND \
			 				education.year = 2017 AND unemployment.year = 2017 AND population.year = 2017 AND poverty.year = 2017 AND health.year = 2017 AND MOD(index.fips,1000) != 0 AND index.state = %s ORDER BY index.fips;",(state,))
			return cursor.fetchall()

	###HEATMAP CHANGE QUERIES------------------------------------------------------------------------------------------------------------
	 #Again, even though format is being used, year is hardcoded into function call.  Used to reduce copied code.
	def demo_change_heat_query(self, state, year):
		with self.conn.cursor() as cursor:
			if state == "ALL":
				cursor.execute("SELECT hsgraduationrate, unemployedpercent, populationestimate, CAST(births AS FLOAT)/ CAST(populationestimate AS FLOAT) birthrate,\
			 				CAST(deaths AS FLOAT)/CAST(populationestimate AS FLOAT) deathrate FROM education, unemployment, \
			 				population, index WHERE index.fips = population.fips AND index.fips = unemployment.fips AND index.fips = education.fips AND \
			 				education.year = {:} AND unemployment.year = {:} AND population.year = {:} AND index.fips % 1000 != 0 ORDER BY index.fips;".format(year,year,year))
			else:
				cursor.execute("SELECT hsgraduationrate, unemployedpercent, populationestimate, CAST(births AS FLOAT)/ CAST(populationestimate AS FLOAT) birthrate,\
			 				CAST(deaths AS FLOAT)/CAST(populationestimate AS FLOAT) deathrate FROM education, unemployment, \
			 				population, index WHERE index.fips = population.fips AND index.fips = unemployment.fips AND index.fips = education.fips AND \
			 				index.state = %s AND education.year = {:} AND unemployment.year = {:} AND population.year = {:} AND MOD(index.fips,1000) != 0 ORDER BY index.fips;".format(year,year,year), (state,))
			return cursor.fetchall()

	def health_change_heat_query(self, state,year):
		with self.conn.cursor() as cursor:
			if state == "ALL":
				cursor.execute("SELECT ypll, poorhealth, adultsmokingpercent, adultobesitypercent, bingedrinkingpercent, lowbirthweightpercent, stirate, teenbirthrate, uninsuredpercent \
								FROM health, index WHERE index.fips = health.fips AND health.year = {:};".format(year))
			else:
				cursor.execute("SELECT ypll, poorhealth, adultsmokingpercent, adultobesitypercent, bingedrinkingpercent, lowbirthweightpercent, stirate, teenbirthrate, uninsuredpercent \
								FROM health, index WHERE index.fips = health.fips AND index.state = %s AND health.year = {:};".format(year), (state,))
			return cursor.fetchall()

	def both_change_heat_query(self, state, year):
		with self.conn.cursor() as cursor:
			if state == "ALL":
				cursor.execute("SELECT hsgraduationrate, unemployedpercent, populationestimate, CAST(births AS FLOAT)/ CAST(populationestimate AS FLOAT) birthrate,\
			 				CAST(deaths AS FLOAT)/CAST(populationestimate AS FLOAT) deathrate, \
			 				ypll, poorhealth, adultsmokingpercent, adultobesitypercent, bingedrinkingpercent, lowbirthweightpercent, stirate, \
			 				teenbirthrate, uninsuredpercent FROM education, unemployment, population, health, index \
			 				WHERE index.fips = population.fips AND index.fips = unemployment.fips AND index.fips = education.fips AND index.fips = health.fips AND \
			 				education.year = {:} AND unemployment.year = {:} AND population.year = {:} AND health.year = {:} AND index.fips % 1000 != 0 ORDER BY index.fips;".format(year, year, year, year))
			else:
				cursor.execute("SELECT hsgraduationrate, unemployedpercent, populationestimate, CAST(births AS FLOAT)/ CAST(populationestimate AS FLOAT) birthrate,\
			 				CAST(deaths AS FLOAT)/CAST(populationestimate AS FLOAT) deathrate, \
			 				ypll, poorhealth, adultsmokingpercent, adultobesitypercent, bingedrinkingpercent, lowbirthweightpercent, stirate, \
			 				teenbirthrate, uninsuredpercent FROM education, unemployment, population, health, index \
			 				WHERE index.fips = population.fips AND index.fips = unemployment.fips AND index.fips = education.fips AND index.fips = health.fips AND \
			 				education.year = {:} AND unemployment.year = {:} AND population.year = {:} AND health.year = {:} AND MOD(index.fips,1000) != 0 AND index.state = %s \
			 				ORDER BY index.fips;".format(year, year, year, year),(state,))
			return cursor.fetchall()

	### TOP/BOTTOM N QUERY------------------------------------------------------------------------------------------------------------
	# Demo/relation are picked by the user supplying integers, pulled from hardcoded lists.  N is cast as an int in application.py , so any alphabetic value fails.
	# bot (bottom or top N) is picked from hard coded values as well - No user string input is passed directly into query - no SQL injection.
	def tb_n_query(self, relation, demo, state, bot, N):
		with self.conn.cursor() as cursor:
			if state == "ALL":
				cursor.execute("SELECT index.county, index.state, {:} FROM {:}, index WHERE index.fips = {:}.fips AND {:}.year = 2017 AND {:}.{:} IS NOT NULL ORDER BY {:} {:} LIMIT {:};".format(demo,relation, relation, relation,relation, demo, demo, bot,N))
			else:
				cursor.execute("SELECT index.county, index.state, {:} FROM {:}, index WHERE index.state = %s AND index.fips = {:}.fips AND {:}.year = 2017 AND {:}.{:} IS NOT NULL ORDER BY {:} {:} LIMIT {:};".format(demo,relation, relation, relation, relation, demo, demo, bot,N),(state,))
			d17 = cursor.fetchall()
			if relation != "poverty":
				if state == "ALL":
					cursor.execute("SELECT index.county, index.state, {:} FROM {:}, index WHERE index.fips = {:}.fips AND {:}.year = 2010 AND {:}.{:} IS NOT NULL ORDER BY {:} {:} LIMIT {:};".format(demo,relation, relation, relation,relation, demo, demo, bot,N))
				else:
					cursor.execute("SELECT index.county, index.state, {:} FROM {:}, index WHERE index.state = %s AND index.fips = {:}.fips AND {:}.year = 2010 AND {:}.{:} IS NOT NULL ORDER BY {:} {:} LIMIT {:};".format(demo,relation, relation, relation, relation, demo, demo, bot,N), (state,))
				d10 = cursor.fetchall()
				out = []
				for i in range(min(len(d10),len(d17))):
					out.append([d10[i][0],d10[i][1],d10[i][2] ,d17[i][0],d17[i][1],d17[i][2]])
				if min(len(d10),len(d17)) == len(d10):
					for i in range(len(d10),len(d17)):
						out.append([None, None,None, d17[i][0],d17[i][1],d17[i][2]])
				else:
					for i in range(len(d17),len(d10)):
						out.append([d10[i][0],d10[i][1],d10[i][2],None, None, None])
				return out
			return d17

	### COPMARE RANKINGS IN STATE QUERY------------------------------------------------------------------------------------------------------------
	def comp_rank(self, f1, f2, year):
		with self.conn.cursor() as cursor:
			cursor.execute("SELECT index.county, outcomes, lengthoflife, qualityoflife, factors, healthbehaviors, clinicalcare, socioeconomic, physicalenvironment FROM index, rankings, subrankings WHERE rankings.year = %s\
							AND subrankings.year = %s AND index.fips = rankings.fips AND index.fips = subrankings.fips AND(index.fips = %s or index.fips = %s) ORDER BY index.fips;",(year,year,f1,f2))
			return cursor.fetchall()

	def get_num_counties(self,state, year):
		with self.conn.cursor() as cursor:
			cursor.execute("SELECT max(outcomes) FROM rankings, index WHERE index.state = %s AND index.fips = rankings.fips AND rankings.year = %s;",(state,year))
			return cursor.fetchall()

	### COLOR MAP QUERY------------------------------------------------------------------------------------------------------------
	# Again, even though format is being used, all parameters are hardcoded into application.py with values selected by user typing ints.
	def color_query(self, rel, attr, year):
		with self.conn.cursor() as cursor:
			cursor.execute("SELECT fips, {:} FROM {:} WHERE year = {:} AND MOD(fips, 1000) != 0 AND {:} IS NOT NULL".format(attr,rel,year, attr))
			return cursor.fetchall()

	def ruc_query(self):
		with self.conn.cursor() as cursor:
			cursor.execute("SELECT fips, ruralurbancont FROM index WHERE MOD(fips, 1000) != 0 AND ruralurbancont IS NOT NULL;")
			return cursor.fetchall()

def set_up():
	print("Opening connection to databse.")
	a = Database('chsi_usda_ers','chsi','chsi')

	print("Connection opened. Clearing existing relations.")
	a.set_up()

	print("Relations cleared. Start populating tables.")
	a.set_index("PopulationEstimates.csv")

	print("Start populating demographic tables.")
	a.population("PopulationEstimates.csv")

	a.poverty17("PovertyEstimates.csv")
	a.povertyU17("2017CHR_CSV_Analytic_Data.csv")

	a.unemployment("Unemployment.csv")

	a.education10("analytic_data2010.csv")
	a.education17("2017CHR_CSV_Analytic_Data.csv")

	print("Demographic relations populated.  Start reading helath data.")
	a.health10("2010 County Health Rankings National Data_v2.xls")
	a.healthU10("analytic_data2010.csv")
	a.health17("2017CountyHealthRankingsData.xls")
	a.healthU17("2017CHR_CSV_Analytic_Data.csv")

	a.rankings("2010 County Health Rankings National Data_v2.xls", 2010)
	a.rankings("2017CountyHealthRankingsData.xls", 2017)
	print("Health tables populated.")

	print("SUCCESS: Database populated.")

	A = Attributes(a)
	return A

def set_up_fast():
	a = Database('chsi_usda_ers','chsi','chsi')
	A = Attributes(a)
	return A


if __name__ == '__main__':
	set_up()
