import random
import database
import pandas 
import sys
import numpy as np


from matplotlib import pyplot as plt
from matplotlib import cm as cm


CATEGORIES_DEMO = ["education", "unemployment", "population", "poverty", "All"]
CATEGORIES_DEMO_SUB = [["HS Enrollment", "HS Diplomas", "HS Graduation Rate"],["Unemployed", "Labor Force", "Unemployment %"],\
					   ["Population", "Births", "Deaths", "International Migrants", "Domestic Migrants"], ["Pop. in Poverty", \
						"Poverty %", "% Children in Poverty", "% Under 5 in Poverty", "Median Household Income", "% Single Parent Houshold", "GINI Coeff"]]
CATEGORIES_DEMO_EX = [["highschoolenrollment", "hsdiplomas", "hsgraduationrate"],["unemployed", "laborforce", "unemployedpercent"],\
					   ["populationestimate", "births", "deaths", "internationalmigrants", "domesticmigrants"], ["povertypopulation", \
						"percentpovertypop", "percentpovertychildren", "percentpovertyunderfive", "medianhouseholdinc", "singleparentpercent", "ginicoefficient"]]
CATEGORIES_HEALTH = ["General Health", "Substance/Crime Information", "Birth Health", "Medical Care Access", "All"]
CATEGORIES_HEALTH_SUB = [["Years Potential Life Lost/100,000", "% With Poor/Fair Health", "AVG Poor Health Days/Month", "AVG Poor Mental Health Days/Month", "Pollution"],\
						["% Adult Smokers", "% Obese Adults", "% Binge Drinkers", "Liquor Store Density", "Violent Crime Rate"],\
						["% Low Birth Weight", "STI Rate/100,000", "Teen Birth Rate"],["% Uninsured", "Primary Care Physicians/100,000", "Preventable Hospital Stays/1,000", "Diabetics Screening"]]
CATEGORIES_HEALTH_EX = [["ypll", "poorhealth", "poorphysicaldays", "poormentaldays", "pollution"],\
						["adultsmokingpercent", "adultobesitypercent", "bingedrinkingpercent", "liquorstoredensity", "violentcrimerate"],\
						["lowbirthweightpercent", "stirate", "teenbirthrate"],["uninsuredpercent", "pcprate", "preventablehospitalstays", "diabeticscreening"]]

CATEGORIES_RANK = ["Health Outcomes",  "Length of Life", "Quality of Life", "Health Factors", "Healthy Behaviors", "Clinical Care", "Socioeconomic Factors", "Physical Environment"]
CATEGORIES_RANK_EX = ["outcomes", "lengthoflife", "qualityoflife", "factors", "healthbehaviors", "clinicalcare", "socioeconomic", "physicalenvironment" ]

SCHEMA_1 = [["fips", "fips", "fips", "fips", "fips"],\
			["state", "year", "year", "year", "year"],\
			["county", "highschoolenrollment", "laborforce", "census", "povertypopulation"],\
			["ruralurbancont", "hsdiplomas", "employed", "populationestimate", "percentpovertypop" ],\
			[" ", "hsgraduationrate", "unemployed", "births", "povertychildren"],\
			[" ", " ", "unemployedpercent", "deaths", "percentpovertychildren"],\
			[" ", " ", " ", "internationalmigrants", "povertyunderfive"],\
			[" ", " ", " ", "domesticmigrants", "percentpovertyunderfive"],\
			[" ", " ", " ", " ", "medianhouseholdinc"],\
			[" ", " ", " ", " ", "singleparentpercent"],\
			[" ", " ", " ", " ", "ginicoefficient"]]
SCHEMA_2 = [["fips", "fips", "fips", "fips", "fips"],\
			["year", "year", "year", "year", "year"],\
			["ypll", "ypll", "outcomes", "lengthoflife", "lengthoflife"],\
			["poorhealth", "poorhealth", "outcomesquartile", "qualityoflife", "qualityoflife"],\
			["poorphysicaldays", "poorphysicaldays", "factors", "healthbehaviors", "healthbehaviors"],\
			["poormentaldays", "poormentaldays", "factorsquartile", "clincalcare", "clincalcare"],\
			["lowbirthweightpercent", "lowbirthweightpercent", " ", "socioeconomic", "socioeconomic"],\
			["adultsmokingpercent", "adultsmokingpercent", " ", "physicalenviornment", "physicalenviornment"],\
			["adultobesitypercent", "adultobesitypercent", " ", " ", " "],\
			["bingedrinkingpercent", "bingedrinkingpercent", " ", " ", " "],\
			["stirate", "tirate", " ", " ", " "],\
			["teenbirthrate", "teenbirthrate", " ", " ", " "],\
			["uninsuredpercent", "uninsuredpercent", " ", " ", " "],\
			["pcprate", "pcprate", " ", " ", " "],\
			["preventablehospitalstays", "preventablehospitalstays", " ", " ", " "],\
			["diabeticscreening", "diabeticscreening", " ", " ", " "],\
			["violentcrimerate", "violentcrimerate", " ", " ", " "],\
			["pollution", "pollution", " ", " ", " "],\
			["liquorstoredensity", "liquorstoredensity", " ", " ", " "]]

class Query():
	def __init__(self, attr):
		self.A = attr
		self.startup()

	def startup(self):
		print("\n\nFinal Project for Database Systems 2019")
		print("by Trevor Maxfield, Greg Stewart, Eric Xu, Luis Garcia\n\n")
		i = input("Would you like to read about the data? (n for no, s for skip) " )
		if i.lower() == 's':
			return
		if i.lower() != 'n':
			print("-------------------------------------------ABOUT THE DATA-------------------------------------------")
			print("This program allows a user to interact with two datasets:\n")
			print("1. CHR (County Health Rankings) Data")
			print("\tPublished by the University of Wisconsin Population Health Institute and the Robert Wood")
			print("\tJohnson Foundation, it measures health and safety factors for every county in the United")
			print("\tStates, designated by FIPS (Federal Information Processing Standard) codes.  Many factors")
			print("\tare reported, everything from percentages in poor health to liquor store density to")
			print("\tpollution for each FIPS code can be found in the data.  Along with the numerical data,")
			print("\tbuilt-in rankings and subrankings are provided to rank each county in it's state.")
			print("\tThis gives one a good idea of how a county compares to those around it.  These results")
			print("\tare published every year, and more information, as well as the data sets for the current")
			print("\tyear and past years, can be found at:")
			print("\thttp://www.countyhealthrankings.org/explore-health-rankings/rankings-data-documentation\n")
		
			print("2. USDA ERS(Economic Research Service) Data")
			print("\tThese County-Level Datasets include basic demographic information for each year")
			print("\tfrom 2010 to 2017 that reports population, unemployment, poverty, and education data")
			print("\tfor each FIPS code.  Also included are Rural-Urban Continuum Codes for each FIPS which,")
			print('\t"distinguish metropolitan (metro) counties by the population size of their metro area,')
			print('\tand nonmetropolitan (nonmetro) counties by degree of urbanization and adjacency')
			print('\tto metro areas", from the ERS website.  The codes go as follows:')
			print("\tMETRO AREAS:")
			print("\t\t1. Counties in metro areas of 1 million population or more")
			print("\t\t2. Counties in metro areas of 250,000 to 1 million population")
			print("\t\t3. Counties in metro areas of fewer than 250,000 population")
			print("\tNONMETRO AREAS:")
			print("\t\t4. Urban population of 20,000 or more, adjacent to a metro area")
			print("\t\t5. Urban population of 20,000 or more, not adjacent to a metro area")
			print("\t\t6. Urban population of 2,500 to 19,999, adjacent to a metro area")
			print("\t\t7. Urban population of 2,500 to 19,999, not adjacent to a metro area")
			print("\t\t8. Completely rural or less than 2,500 urban population, adjacent to a metro area")
			print("\t\t9. Completely rural or less than 2,500 urban population, not adjacent to a metro area")
			print("\tAs of 2013, the distribution of the RUC in the United States/PR:\n")
			print(pandas.DataFrame([[432, 168523961],[379, 65609956], [356, 28318215], [214, 13538322], \
									[92, 4953810], [593, 14784976], [433, 8248674], [220, 2157448], \
									[424, 2610176]], list(range(1,10)),["Number of Counties", "2010 Population"]))
			print("\n\tMore information about the data can be found at:")
			print("\thttps://www.ers.usda.gov/data-products/county-level-data-sets/\n")
			print("Using the intersection of the FIPS codes between both sets of data, we were able to")
			print("compare the demographics of a region along with its health values.\n")

		i = input("Would you like to read about the schema used? (n for no) ")
		if i.lower() != 'n':
			print("------------------------------------------ABOUT THE SCHEMA------------------------------------------")
			print("In all, 10 relations were used to store the data.  FIPS codes intersected each data set and were")
			print("thus used as keys between each data set.  Each relation stored two tuples for each FIPS as data")
			print("from 2010 and 2017 were stored in each table, identified by the year attribute:")
			print("\t1. index  - A county's rural-urban code, FIPS code, and identifying state/county strings.")
			print("\t2. education - A county's graduation rate along with enrollment and diplomas.")
			print("\t3. unemployment - A county's unemployment rate along with labor force and unemployed count.")
			print("\t4. population - A county's population as well as births, deaths, and migration.")
			print("\t5. poverty - A county's poverty rates for different groups, median income, GINI coeff, etc.")
			print("\t6. health - Stored all relevant health data for a FIPS")
			print("\t7. healthquartile - Same attributes as health, only stores a FIPS within-state quartile rank for each attribute.")
			print("\t8. rankings - A county's rank within its state.")
			print("\t9. subrankings - A county's rank within the subrankings that make up the rankings.")
			print("\t10. subrankingsquartiles - A county's within-state quartile rank for each subranking.\n")

		i = input("Would you like to see a representation of the relations? (n for no) ")
		if i.lower() != 'n':
			print("---------------------------------------------THE SCHEMA---------------------------------------------")
			print(pandas.DataFrame(SCHEMA_1, columns=["index","education","unemployment","population","poverty"]))
			print()
			print(pandas.DataFrame(SCHEMA_2, columns=["health", "healthquartile", "rankings", "subrankings", "subrankingsquartiles"]))
			print()
			print()

		i = input("Would you like to read about the data loading code? (n for no) ")
		if i.lower() != 'n':
			print("--------------------------------------------DATA LOADING--------------------------------------------")
			print("Loading the data can be done through the code in load_data.py or alternatively by running set_up()")
			print("in database.py with pandas, csv, and psycopg2 installed.  All csv and xls files from requirements.txt")
			print("must be downloaded to the same directory, and the code in db_setup.sql must be first run as a psql")
			print("admin.  From there, load_data.py takes about a minute or so to run.\n")

		i = input("Would you like to read about how we avoided SQL injection vulnerabilities? (n for no) ")
		if i.lower() != 'n':
			print("--------------------------------------------SQL INJECTION-------------------------------------------")
			print("In order to avoid SQL injection, pyscopg2's execute parameter passing is utilized as much as")
			print("possible.  In some instances (ie to avoid hardcoding hundreds of query combinations) where this")
			print("was not viable, queries were formatted using strings passed from application.py to database.py.")
			print("However, these strings are ALL hardcoded into application.py, such that no user input is ever")
			print("sent to a query.  Rather ints or single letters are used as input and then if statements will pull")
			print("corresponding values from arrays of strings.")
		i = input("Would you like to read a description of each query? (n for no) ")
		if i.lower() != 'n':
			print("--------------------------------------------THE QUERIES--------------------------------------------")


	'''
	Prompts the user to pick a state, returns (state, stateAB)
	Safe for sql queries, as input selects a state rather than is a state.
	'''
	def pick_state(self):
		state = (-1,-1)
		self.A.print_states()
		while state[0] == -1:
			stin = input("\nPlease choose a state from above (r for random): ")
			if stin.lower() == 'r':
				state = (1, self.A.st[random.randint(0, len(self.A.st))-1])
				while state[1] == "Alaska" or state[1] == "Puerto Rico" or state[1] == "District of Colombia":
					state = (1, self.A.st[random.randint(0, len(self.A.st))-1]) 
				print(state[1], "was selected at random.")
				break
			state = self.A.check_state(stin)
		return self.A.get_state(state[1])

	'''
	Prompts the user to pick a fips code, returns (fips, county, (state, stateAB))
	Safe for sql queries as input must be valid, user input is used to select a fips.
	'''
	def pick_fips(self, state):
		fips_list = self.A.get_fips_state(state[1])
		fips_list.sort()
		fips = -1
		while fips == -1:
			print()
			for i in fips_list:
				print("{:}: {:}".format(i[0],i[1]))
			fips = input("\nPlease choose a fips code from above (r for random): ")
			if fips.lower() == 'r':
				fips = fips_list[random.randint(0,len(fips_list))-1]
				print("{:}: {:} was selected at random.".format(fips[0],fips[1].strip()))
				print("{:}: {:}, {:}".format(fips[0],fips[1].strip(),state[0].strip()))
				return (fips[0], fips[1], state)
			try:
				fips = int(fips)
			except ValueError:
				fips = -1
				continue
			for i in fips_list:
				if i[0] == fips:
					print("{:}: {:}, {:}".format(fips,i[1].strip(),state[0].strip()))
					return (fips, i[1], state)
			fips = -1
			print("ERROR: Invalid FIPS code.  Please choose from the list")
		print()

	'''
	Lets the user compare two fips (f1,f2) over the selected demographic
	'''
	def compare_two_fips_demo(self, f1, f2):
		print("What would you like to compare {:}, {:} and {:}, {:} on?".format(f1[1].title(), f1[2][0], f2[1].title(), f2[2][0]))
		t = 1
		for i in CATEGORIES_DEMO:
			print("{:}. {:}".format(t,i.title()))
			t += 1
		cat = 0
		while cat < 1 or cat > len(CATEGORIES_DEMO):
			cat = input("\nChoose a category (number) or r for random: ")
			if cat == 'q':
				return
			if cat == 'r':
				cat = random.randint(1,len(CATEGORIES_DEMO))
			try:
				cat = int(cat)
			except ValueError:
				cat = -1
				continue
		if cat == len(CATEGORIES_DEMO):
			cat = -1
		if cat == 1 or cat == -1:
			print("-----{:}{:}".format(CATEGORIES_DEMO[0].title(),'-'*100))
			data = self.A.ed_query(f1[0],f2[0])
			d = shape_demo(data, 3, f1, f2)
			print_demo(d,f1,f2,["HS Enrollment", "HS Diplomas", "HS Graduation Rate"])
		if cat == 2 or cat == -1:
			print("-----{:}{:}".format(CATEGORIES_DEMO[1].title(),'-'*100))
			data = self.A.un_query(f1[0],f2[0])
			d = shape_demo(data, 3, f1, f2)
			print_demo(d,f1,f2,["Unemployed", "Labor Force", "Unemployment %"])
		if cat == 3 or cat == -1:
			print("-----{:}{:}".format(CATEGORIES_DEMO[2].title(),'-'*100))
			data = self.A.pop_query(f1[0],f2[0])
			d = shape_demo(data, 5, f1, f2)
			print_demo(d,f1,f2,["Population", "Births", "Deaths", "International Migrants", "Domestic Migrants"])
		if cat == 4 or cat == -1:
			print("-----{:}{:}".format(CATEGORIES_DEMO[3].title(),'-'*100))
			data = self.A.pov_query(f1[0],f2[0])
			labels = ["Pop. in Poverty", "Poverty %", "% Children in Poverty", "% Under 5 in Poverty", "Median Household Income", "% Single Parent Houshold", "GINI Coeff"]
			n = len(labels)
			dF117 = [None]*n
			dF217 = [None]*n
			for i in data:
				if i[0] == f1[0]:
					dF117 = i[2:2+n]
				elif i[0] == f2[0]:
					dF217 = i[2:2+n]
			print("2017:")
			print(pandas.DataFrame([dF117,dF217], ["{:}, {:}".format(f1[1].title(), f1[2][0]),"{:}, {:}".format(f2[1].title(), f2[2][0])], labels))
			print()

	'''
	Lets the user compare two fips (f1, f2) over the selected health factors
	'''
	def compare_two_fips_health(self, f1, f2):
		print("What would you like to compare {:}, {:} and {:}, {:} on?".format(f1[1].title(), f1[2][0], f2[1].title(), f2[2][0]))
		t = 1
		for i in CATEGORIES_HEALTH:
			print("{:}. {:}".format(t,i.title()))
			t += 1
		cat = 0
		while cat < 1 or cat > len(CATEGORIES_HEALTH):
			cat = input("\nChoose a category (number) or r for random: ")
			if cat == 'q':
				return
			if cat == 'r':
				cat = random.randint(1,len(CATEGORIES_HEALTH))
			try:
				cat = int(cat)
			except ValueError:
				cat = -1
				continue
		if cat == len(CATEGORIES_HEALTH):
			cat = -1
		if cat == 1 or cat == -1:
			print("-----{:}{:}".format(CATEGORIES_HEALTH[0],'-'*100))
			data = self.A.gh_query(f1[0],f2[0])
			d = shape_demo(data, 5, f1, f2)
			print_demo(d,f1,f2,["Years Potential Life Lost/100,000", "% With Poor/Fair Health", "AVG Poor Health Days/Month", "AVG Poor Mental Health Days/Month", "Pollution"])
		if cat == 2 or cat == -1:
			print("-----{:}{:}".format(CATEGORIES_HEALTH[1],'-'*100))
			data = self.A.sub_query(f1[0],f2[0])
			d = shape_demo(data, 5, f1, f2)
			print_demo(d,f1,f2,["% Adult Smokers", "% Obese Adults", "% Binge Drinkers", "Liquor Store Density", "Violent Crime Rate"])
		if cat == 3 or cat == -1:
			print("-----{:}{:}".format(CATEGORIES_HEALTH[2],'-'*100))
			data = self.A.bh_query(f1[0],f2[0])
			d = shape_demo(data, 3, f1, f2)
			print_demo(d,f1,f2,["% Low Birth Weight", "STI Rate/100,000", "Teen Birth Rate"])
		if cat == 4 or cat == -1:
			print("-----{:}{:}".format(CATEGORIES_HEALTH[3],'-'*100))
			data = self.A.mc_query(f1[0],f2[0])
			d = shape_demo(data, 4, f1, f2)
			print_demo(d,f1,f2,["% Uninsured", "Primary Care Physicians/100,000", "Preventable Hospital Stays/1,000", "Diabetics Screening"])

	'''
	Shows the rankings of the counties/fips in a state for a selected ranking/subranking.
	'''
	def rank_state(self, state):
		print("\nWhat ranking metric would you like to use for {:}?".format(state[0]))
		print("1. Health Outcomes: Average of Length of Life and Quality of Life")
		print("\t2. Length of Life (50%): Years of potential life lost before age 75/100,000 people.")
		print("\t3. Quality of Life (50%): Percent in poor health with poor physical/mental health days per month and low birthweight percentage.")
		print("4. Health Factors: Weighted combination of the following subfactors")
		print("\t5. Healthy Behaviors (30%): Diet and exercise weighed against unsafe drug/alcohol use and unsafe sexual practices.")
		print("\t6. Clinical Care (20%): Quality of clincal care and access to it.")
		print("\t7. Socioeconomic Factors (40%): Measure of education levels, unemployment, income, familiy support, and community safety.")
		print("\t8. Physical Enviornment (10%): Measure of air/water quality and housing/transit availability.")
		cat = 0
		while cat < 1 or cat > len(CATEGORIES_RANK):
			cat = input("\nChoose a category (number) or r for random: ")
			if cat == 'q':
				return
			if cat == 'r':
				cat = random.randint(1,len(CATEGORIES_RANK)-1)
			try:
				cat = int(cat)
			except ValueError:
				cat = -1
				continue
		rel = "subrankings"
		if cat == 1 or cat == 4:
			rel = "rankings"
		print("-----{:}: {:}".format(CATEGORIES_RANK[cat-1], state[0]))
		data = self.A.rank_query(state[1], CATEGORIES_RANK_EX[cat-1], rel)
		data = net_change_rank(data)
		print(pandas.DataFrame(data, list(range(1,len(data)+1)), ["2010","2017","Change"]))

	'''
	Displays a heatmap of correlation between attributes over a state/US
	f = 1: demographics
	f = 2: health
	f = 3: both
	'''
	def self_heat_corr(self, f, state):
		data = []
		df = []
		title = 'Demographic Feature Correlation - {:}'.format(state[0])
		l1=['Graduation Rate', 'Unemployment', 'Population', 'Birth Rate', 'Death Rate', 'Poverty Rate', 'Pov Under 5 Rate', 'Median Household Income', 'Rural Urban Continuum']
		l2 = l1
		if f == 1:
			data = self.A.demo_heat_query(state[1])
			df = pandas.DataFrame(data).corr()
		elif f == 2:
			data = self.A.health_heat_query(state[1])
			df = pandas.DataFrame(data).corr()
			title = 'Health Feature Correlation - {:}'.format(state[0])
			l1=['YPLL', 'Poor Health Rate', 'Pollution', 'Adult Smoking %', 'Adult Obesity %', 'Binge Drinking %', '% Low Birthweight', 'STI Rate', 'Teen Birth Rate', 'Uninsured Percent']
			l2 = l1
		elif f == 3:
			l2 = ['YPLL', 'Poor Health Rate', 'Pollution', 'Adult Smoking %', 'Adult Obesity %', 'Binge Drinking %', '% Low Birthweight', 'STI Rate', 'Teen Birth Rate', 'Uninsured Percent']
			data = self.A.both_heat_query(state[1])
			data = pandas.DataFrame(data, columns = l1+l2).corr()
			df = data.iloc[0:len(l1),len(l1):]
			title = 'Health-Demographic Feature Correlation - {:}'.format(state[0])
		fig = plt.figure()
		ax1 = fig.add_subplot(111)
		cmap = cm.get_cmap('jet', 30)
		cax = ax1.imshow(df.abs(), interpolation="nearest", cmap=cmap)
		ax1.grid(True)
		plt.title(title)
		ax1.set_xticks(range(len(l2)))
		ax1.set_yticks(range(len(l1)))
		ax1.set_xticklabels(l2,fontsize=12,rotation='vertical')
		ax1.set_yticklabels(l1,fontsize=12)
		# Add colorbar, make sure to specify tick locations to match desired ticklabels
		fig.colorbar(cax, ticks=[-1,-.75,-.5,-.25,0,.25,.50,.75,1])
		plt.show()

	'''
	Displays a heatmap of correlation between change in attributes over time fot a state/the US
	f = 1: demographics
	f = 2: health
	f = 3: both
	'''
	def self_change_heat_corr(self, f, state):
		data = []
		df = []
		title = 'Demographic Change Correlation - {:}'.format(state[0])
		l1=['Graduation Rate', 'Unemployment', 'Population', 'Birth Rate', 'Death Rate']
		l2 = l1
		if f == 1:
			data10 = pandas.DataFrame(self.A.demo_change_heat_query(state[1],2010))
			data17 = pandas.DataFrame(self.A.demo_change_heat_query(state[1],2017))
			data = (data17 - data10)
			df = data.corr()
		elif f == 2:
			data10 = pandas.DataFrame(self.A.health_change_heat_query(state[1],2010))
			data17 = pandas.DataFrame(self.A.health_change_heat_query(state[1],2017))
			data = (data17 - data10)
			df = pandas.DataFrame(data).corr()
			title = 'Health Change Correlation - {:}'.format(state[0])
			l1=['YPLL', 'Poor Health Rate', 'Adult Smoking %', 'Adult Obesity %', 'Binge Drinking %', '% Low Birthweight', 'STI Rate', 'Teen Birth Rate', 'Uninsured Percent']
			l2 = l1
		elif f == 3:
			l2 = ['YPLL', 'Poor Health Rate', 'Adult Smoking %', 'Adult Obesity %', 'Binge Drinking %', '% Low Birthweight', 'STI Rate', 'Teen Birth Rate', 'Uninsured Percent']
			data10 = pandas.DataFrame(self.A.both_change_heat_query(state[1],2010), columns = l1+l2)
			data17 = pandas.DataFrame(self.A.both_change_heat_query(state[1],2017), columns = l1+l2)
			data = (data17-data10)
			data = data.corr()
			df = data.iloc[0:len(l1),len(l1):]
			title = 'Health-Demographic Change Correlation - {:}'.format(state[0])
		fig = plt.figure()
		ax1 = fig.add_subplot(111)
		cmap = cm.get_cmap('jet', 30)
		cax = ax1.imshow(df.abs(), interpolation="nearest", cmap=cmap)
		ax1.grid(True)
		plt.title(title)
		ax1.set_xticks(range(len(l2)))
		ax1.set_yticks(range(len(l1)))
		ax1.set_xticklabels(l2,fontsize=12,rotation='vertical')
		ax1.set_yticklabels(l1,fontsize=12)
		# Add colorbar, make sure to specify tick locations to match desired ticklabels
		fig.colorbar(cax, ticks=[0,.25,.50,.75,1])
		plt.show()

	"""
	Returns the top or bottom n fips for a given demographic over the US or a single state.
	"""
	def top_n_demo(self, state):
		print("\nWhich category would you like to see?")
		for i in range(len(CATEGORIES_DEMO)-1):
			print("{:}. {:}".format(i+1,CATEGORIES_DEMO[i].title()))
		cat = 0
		while cat < 1 or cat > len(CATEGORIES_DEMO)-1:
			cat = input("\nChoose a category (number), r for random, or q to quit: ")
			if cat == 'q':
				return
			if cat == 'r':
				cat = random.randint(1,len(CATEGORIES_DEMO)-1)
			try:
				cat = int(cat)
			except ValueError:
				cat = -1
		print("Which attribute would you like to see?")
		for i in range(len(CATEGORIES_DEMO_SUB[cat-1])):
			print("{:}. {:}".format(i+1,CATEGORIES_DEMO_SUB[cat-1][i]))
		dem = 0
		while dem < 1 or dem > len(CATEGORIES_DEMO_SUB[cat-1]):
			dem = input("\nChoose an attribute (number), r for random, or q to quit: ")
			if dem == 'q':
				return
			if dem == 'r':
				dem = random.randint(1,len(CATEGORIES_DEMO_SUB)-1)
			try:
				dem = int(dem)
			except ValueError:
				dem = -1
		N = 0
		tb = input("See the top or bottom N? (b for bottom, else top) ")
		bot = ("top","DESC")
		if tb.lower() == 'b':
			bot = ("bottom","ASC")
		if tb == 'q':
			return
		while N < 1:
			N = input("Select the {:} N? ".format(bot[0]))
			if N == 'q':
				return
			if N == 'r':
				N = random.randint(1,20)
			try:
				N = int(N)
			except ValueError:
				N = -1
		data = self.A.tb_n_query(CATEGORIES_DEMO[cat-1],CATEGORIES_DEMO_EX[cat-1][dem-1],state[1],bot[1],N)
		label = ["County (2010)","State (2010)", CATEGORIES_DEMO_SUB[cat-1][dem-1], "County (2017)","State (2017)", CATEGORIES_DEMO_SUB[cat-1][dem-1]]
		if CATEGORIES_DEMO[cat-1] == "poverty":
			label = ["County (2017)","State (2017)", CATEGORIES_DEMO_SUB[cat-1][dem-1]]
		print("Showing the {:} {:} counties in {:} in {:} ".format(bot[0], N, CATEGORIES_DEMO_SUB[cat-1][dem-1], state[0]))
		print(pandas.DataFrame(data,list(range(1,len(data)+1)),label))

	"""
	Returns the top or bottom n fips for a given health attribute over the US or a single state.
	"""
	def top_n_health(self, state):
		print("\nWhich category would you like to see?")
		for i in range(len(CATEGORIES_HEALTH)-1):
			print("{:}. {:}".format(i+1,CATEGORIES_HEALTH[i].title()))
		cat = 0
		while cat < 1 or cat > len(CATEGORIES_HEALTH)-1:
			cat = input("\nChoose a category (number), r for random, or q to quit: ")
			if cat == 'q':
				return
			if cat == 'r':
				cat = random.randint(1,len(CATEGORIES_HEALTH)-1)
			try:
				cat = int(cat)
			except ValueError:
				cat = -1
		print("Which attribute would you like to see?")
		for i in range(len(CATEGORIES_HEALTH_SUB[cat-1])):
			print("{:}. {:}".format(i+1,CATEGORIES_HEALTH_SUB[cat-1][i]))
		dem = 0
		while dem < 1 or dem > len(CATEGORIES_HEALTH_SUB[cat-1]):
			dem = input("\nChoose an attribute (number), r for random, or q to quit: ")
			if dem == 'q':
				return
			if dem == 'r':
				dem = random.randint(1,len(CATEGORIES_HEALTH_SUB)-1)
			try:
				dem = int(dem)
			except ValueError:
				dem = -1
		N = 0
		tb = input("See the top or bottom N? (b for bottom, else top) ")
		bot = ("top","DESC")
		if tb.lower() == 'b':
			bot = ("bottom","ASC")
		if tb == 'q':
			return
		while N < 1:
			N = input("Select the {:} N? ".format(bot[0]))
			if N == 'q':
				return
			if N == 'r':
				N = random.randint(1,20)
			try:
				N = int(N)
			except ValueError:
				N = -1
		data = self.A.tb_n_query("health", CATEGORIES_HEALTH_EX[cat-1][dem-1],state[1],bot[1],N)
		label = ["County (2010)","State (2010)", CATEGORIES_HEALTH_SUB[cat-1][dem-1], "County (2017)","State(2017)", CATEGORIES_HEALTH_SUB[cat-1][dem-1]]
		print("Showing the {:} {:} counties in {:} in {:} ".format(bot[0], N, CATEGORIES_HEALTH_SUB[cat-1][dem-1], state[0]))
		print(pandas.DataFrame(data,list(range(1,len(data)+1)),label))


	'''
	Allow the user to compare the ranking of two fips in a state.
	'''
	def two_fips_rank(self, state):
		first = self.pick_fips(state)
		while first[0] % 1000 == 0:
			print("ERROR: Cannot pick state as FIPS")
			first = self.pick_fips(state)
		second = self.pick_fips(state)
		while second[0] % 1000 == 0 or second == first:
			print("ERROR: Cannot pick state as FIPS or have the same FIPS")
			second = self.pick_fips(state)
		f1 = min(first,second)
		f2 = max(first,second)
		d10 = self.A.comp_rank(f1[0], f2[0], 2010)
		d17 = self.A.comp_rank(f1[0], f2[0], 2017)
		ch = []
		for i in range(2):
			t = []
			for j in range(1,len(d10[i])):
				if d17[i][j] is not None and d10[i][j] is not None:
					t.append(d10[i][j] - d17[i][j])
				else:
					t.append(None)
			ch.append(t)
		print("\nComparing the relative rankings of {:} and {:} in {:} for 2010 (out of {:} ranked counties):".format(f1[1],f2[1],f1[2][0],self.A.get_num_counties(f1[2][1],2010)[0][0]))
		print(pandas.DataFrame([d10[0][1:],d10[1][1:]],[f1[1],f2[1]],CATEGORIES_RANK))
		print("\nComparing the relative rankings of {:} and {:} in {:} for 2017 (out of {:} ranked counties):".format(f1[1],f2[1],f1[2][0],self.A.get_num_counties(f1[2][1],2017)[0][0]))
		print(pandas.DataFrame([d17[0][1:],d17[1][1:]],[f1[1],f2[1]],CATEGORIES_RANK))
		print("\nComparing the relative ranking change of {:} and {:} in {:} for 2010->2017:".format(f1[1],f2[1],f1[2][0]))
		print(pandas.DataFrame(ch,[f1[1],f2[1]],CATEGORIES_RANK))

	'''
	Given data ([Fips,value] matrix) and title information, plots a choropleth (heatmap ontop of US map)
	of the data.
	'''
	def mapper(self, data, ti, li, ti2, quart):
		print("\nGenerating US Heatmap for {:}".format(ti))
		print("This might take a second...")
		if not quart:
			endpts = list(np.mgrid[min(data.iloc[:][1]):max(data.iloc[:][1]):8j])
			print('z'+ti2.replace('/',','))
			fig = ff.create_choropleth(fips=data.iloc[:][0], values=data.iloc[:][1], title = ti,\
									   legend_title = li, binning_endpoints=endpts)
		else:
			fig = ff.create_choropleth(fips=data.iloc[:][0], values=data.iloc[:][1], title = ti,\
									   legend_title = li)
		
		plot(fig, filename= 'z'+ti2.replace('/',','))


	'''
	Gets the data for a given demographic for all counties in the US to create a choropleth
	'''
	def color_map_demo(self):
		print("\nWhich category would you like to see?")
		for i in range(len(CATEGORIES_DEMO)-1):
			print("{:}. {:}".format(i+1,CATEGORIES_DEMO[i].title()))
		cat = 0
		print("ruc: Rural-Urban Continuum")
		while cat < 1 or cat > len(CATEGORIES_DEMO)-1:
			cat = input("\nChoose a category (number or ruc), r for random, or q to quit: ")
			if cat.lower() == 'q':
				return
			elif cat.lower() == 'r':
				cat = random.randint(1,len(CATEGORIES_DEMO)-1)
			elif cat.lower() == 'ruc':
				print("\nGenerating US Heatmap for Rural-Urban Continuum data.")
				print("This might take a second...")
				data = pandas.DataFrame(self.A.ruc_query())
				fig = ff.create_choropleth(fips=data.iloc[:][0], values=data.iloc[:][1], title="Rural-Urban Contiuum Data", legend_title="RUC")
				plot(fig, filename='Choropleth of Rural-Urban Continuum data.html')
				return
			try:
				cat = int(cat)
			except ValueError:
				cat = -1
		print("Which attribute would you like to see?")
		for i in range(len(CATEGORIES_DEMO_SUB[cat-1])):
			print("{:}. {:}".format(i+1,CATEGORIES_DEMO_SUB[cat-1][i]))
		dem = 0
		while dem < 1 or dem > len(CATEGORIES_DEMO_SUB[cat-1]):
			dem = input("\nChoose an attribute (number), r for random, or q to quit: ")
			if dem.lower() == 'q':
				return
			elif dem.lower() == 'r':
				dem = random.randint(1,len(CATEGORIES_DEMO_SUB)-1)
			elif (cat,dem) == (4,'6'):
				print("ERROR: Not enough data")
				dem = -1
			try:
				dem = int(dem)
			except ValueError:
				dem = -1
		y = input("Which year, 2010 or 2017? (Default 2017) ")
		year = 2017
		if (y == '2010' or y == '10' or y == '1') and cat != 4:
			year = 2010
		data = pandas.DataFrame(self.A.color_query(CATEGORIES_DEMO[cat-1],CATEGORIES_DEMO_EX[cat-1][dem-1], year))
		self.mapper(data,"{:} data from {:}".format(CATEGORIES_DEMO_SUB[cat-1][dem-1],year),CATEGORIES_DEMO_SUB[cat-1][dem-1],\
			   '{:}{:}.html'.format(CATEGORIES_DEMO_SUB[cat-1][dem-1],year), False)

	'''
	Use quart ='' if not quartile data, else use ' quartile'
	'''
	def color_map_health(self, quart):
		print("\nWhich category would you like to see?")
		for i in range(len(CATEGORIES_HEALTH)-1):
			print("{:}. {:}".format(i+1,CATEGORIES_HEALTH[i].title()))
		cat = 0
		while cat < 1 or cat > len(CATEGORIES_HEALTH)-1:
			cat = input("\nChoose a category (number), r for random, or q to quit: ")
			if cat.lower() == 'q':
				return
			if cat.lower() == 'r':
				cat = random.randint(1,len(CATEGORIES_HEALTH)-1)
			try:
				cat = int(cat)
			except ValueError:
				cat = -1
		print("Which attribute would you like to see?")
		for i in range(len(CATEGORIES_HEALTH_SUB[cat-1])):
			print("{:}. {:}".format(i+1,CATEGORIES_HEALTH_SUB[cat-1][i]))
		dem = 0
		while dem < 1 or dem > len(CATEGORIES_HEALTH_SUB[cat-1]):
			dem = input("\nChoose an attribute (number), r for random, or q to quit: ")
			if dem.lower() == 'q':
				return
			if dem.lower() == 'r':
				dem = random.randint(1,len(CATEGORIES_HEALTH_SUB)-1)
			try:
				dem = int(dem)
			except ValueError:
				dem = -1
		y = input("Which year, 2010 or 2017? (Default 2017) ")
		year = 2017
		if (y == '2010' or y == '10' or y == '1'):
			year = 2010
		rel = "healthquartile"
		if quart == '':
			rel = "health"
		data = pandas.DataFrame(self.A.color_query(rel,CATEGORIES_HEALTH_EX[cat-1][dem-1], year))
		self.mapper(data,"{:}{:} data from {:}".format(CATEGORIES_HEALTH_SUB[cat-1][dem-1],quart,year), CATEGORIES_HEALTH_SUB[cat-1][dem-1]+quart,\
			   '{:}{:}{:}.html'.format(CATEGORIES_HEALTH_SUB[cat-1][dem-1],quart,year), quart == ' quartile' )

	'''
	Use quart ='' if not quartile data, else use ' quartile'
	'''
	def color_map_rankings(self, quart):
		print("\nWhich category would you like to see?")
		for i in range(len(CATEGORIES_RANK)-1):
			print("{:}. {:}".format(i+1,CATEGORIES_RANK[i].title()))
		cat = 0
		while cat < 1 or cat > len(CATEGORIES_RANK)-1:
			cat = input("\nChoose a category (number), r for random, or q to quit: ")
			if cat.lower() == 'q':
				return
			if cat.lower() == 'r':
				cat = random.randint(1,len(CATEGORIES_RANK)-1)
			try:
				cat = int(cat)
			except ValueError:
				cat = -1
		rel = "subrankings"
		attr = CATEGORIES_RANK_EX[cat-1]
		if cat == 1 or cat == 4:
			rel = "rankings"
		if quart != '' and cat != 1 and cat != 4:
			rel += "quartiles"
		elif quart != '':
			attr+="quartile"
		y = input("Which year, 2010 or 2017? (Default 2017) ")
		year = 2017
		if (y == '2010' or y == '10' or y == '1'):
			year = 2010
		data = pandas.DataFrame(self.A.color_query(rel,attr, year))
		self.mapper(data, "{:}{:} data from {:}".format(CATEGORIES_RANK[cat-1],quart,year), "Rank" + quart, \
			   '{:}{:}{:}.html'.format(CATEGORIES_RANK[cat-1],quart,year), True)



#END CLASS------------------------------------------------------------------------------------------
'''
Takes in the result of a demograpic relation query (fips, year, [n more columns]) and outputs
2D data matrices of the results, based on year in order by FIPS.  A difference of the two
years is also returned, (d2010,d2017,diff)
'''
def shape_demo(data, n, f1, f2):
	dF110 = [None]*n
	dF210 = [None]*n
	dF117 = [None]*n
	dF217 = [None]*n
	for i in data:
		if i[0] == f1[0] and i[1] == 2010:
			dF110 = i[2:2+n]
		elif i[0] == f2[0] and i[1] == 2010:
			dF210 = i[2:2+n]
		elif i[0] == f1[0] and i[1] == 2017:
			dF117 = i[2:2+n]
		elif i[0] == f2[0] and i[1] == 2017:
			dF217 = i[2:2+n]
	d10 = [dF110,dF210]
	d17 = [dF117,dF217]
	diff = [[None]*n,[None]*n]
	for i in range(len(d10)):
		for j in range(len(d10[i])):
			if d10[i][j] is not None and d17[i][j] is not None:
				diff[i][j] = d17[i][j] - d10[i][j]
	return (d10,d17,diff)

'''
Print the data from the demographic relation query.  Input is the data = (d10,d17,diff), f1, f2, labels
where labels is a list of headings for the table.  Must be the same length as the data.
'''
def print_demo(data, f1, f2, labels):
	(d10,d17,diff) = data
	print("2010:")
	print(pandas.DataFrame(d10, ["{:}, {:}".format(f1[1].title(), f1[2][0]),"{:}, {:}".format(f2[1].title(), f2[2][0])], labels))
	print("\n2017:")
	print(pandas.DataFrame(d17, ["{:}, {:}".format(f1[1].title(), f1[2][0]),"{:}, {:}".format(f2[1].title(), f2[2][0])], labels))
	print("\nDifference:")
	print(pandas.DataFrame(diff, ["{:}, {:}".format(f1[1].title(), f1[2][0]),"{:}, {:}".format(f2[1].title(), f2[2][0])], labels))
	print()
	
'''
Take in a 2D list of ranked county data, the first column 2010, second 2017.  Finds the net change in
ranking for each county between 2010 and 2017 and appends it to the county in the 2017 col.
'''
def net_change_rank(data):
	for i in range(len(data)):
		mod = False
		for j in range(len(data)):
			if data[i][1] == data[j][0]:
				if j > i:
					data[i].append("{:}".format(j-i))
				else:
					data[i].append("{:}".format(j-i))
				mod = True
				break
		if not mod:
			data[i].append("-")
	return data


if __name__ == '__main__':
	pandas.set_option("display.max_rows", 25)
	
	#Ask if user has run db_setup to create expected database/user
	setup = input("\n\n\nHave you run the code in db_setup.sql in psql as admin yet? (n for no, else yes) ")
	if setup.lower() == 'n':
		print("\nPlease run the code in db_setup.sql as an admin to create a user/database")
		sys.exit(0)
	#Check for working plotly
	geo = True
	print("Checking to see if plotly package installed...")
	try:
		from plotly.offline import download_plotlyjs, init_notebook_mode, plot
		import plotly.offline as py
		import plotly.figure_factory as ff
		print("Sucess! Choropleth creation enabled.")
	except ImportError:
		print("\nERROR: package plotly could not be found.  Disabling choropleth creation (7).")
		print("See the readme file and the following resources for more information.\n")
		print("https://plot.ly/python/county-choropleth/#fips-and-values")
		print("https://geoffboeing.com/2014/09/using-geopandas-windows/\n")
		geo = False

	print("\nCreating connection to database, checking if relations exist")
	A = database.set_up_fast()
	if not A.exists():
		print("Missing relations, will reload data.")
		A = database.set_up()

	print("Success! Data exists, beginning application.")
	
	Q = Query(A)

	while True:
		print("\n\n\nHow would you like to explore the data?")
		print("1. Compare any two counties.")
		print("2. See the county rankings for any state.")
		print("3. See a heat map of attribute correlation for a state.")
		print("4. See a heat map of attribute change over time correlation for a state.")
		print("5. See the top or bottom ranked counties in a state/US.")
		print("6. See the relative ranking of any two counties in a state.")
		print("7. See a (Choropleth) heatmap of the US for any attribute.")
		i = -1
		while i < 1 or i > 7:
			i = input("\nWhich would you like to do? (r for random, q to quit) ")
			if i == 'q':
				sys.exit(0)
			elif i == 'r':
				i = random.randint(1,7)
			try:
				i = int(i)
			except ValueError:
				i = -1
			if i == 7 and not geo:
				print("ERROR: Unable to find package plotly necessary for choropleth creation.")
				print("See reamdme for plotly installation instructions.\n")
				i = -1
			# Which query to select
		try:
			if i == 1:
				input("\nSelection 1: Comparing two counties. Please select counties (enter to continue): ")
				first = Q.pick_fips(Q.pick_state())
				input("Select a second fips (enter to continue): ")
				second = Q.pick_fips(Q.pick_state())
				while second == first:
					print("ERROR: Same FIPS selected, choose another.")
					second = Q.pick_fips(Q.pick_state())
				f1 = min(first,second)
				f2 = max(first,second)
				while True :
					j = input("\nWould you like to compare demographics or health?(h for health, q to quit, r for random) ")
					if j.lower() == 'q':
						break
					elif j.lower() == 'r':
						if random.randint(1,2) == 1:
							j = 'h'
					if j.lower() == 'h':
						Q.compare_two_fips_health(f1, f2)
					else:
						Q.compare_two_fips_demo(f1, f2)

			elif i == 2:
				input("\nSelection 2: County rankings for any state.  Please select a state (enter to continue): ")
				state = Q.pick_state() #(State, AB)
				while state[1] == "US" or state[1] == "PR" or state[1] == "DC":
					print("ERROR: No ranking data for state: {:}".format(state[0]))
					state = Q.pick_state()
				while True:
					Q.rank_state(state)
					i = input("\nChoose another ranking?(q to quit) ")
					if i == 'q':
						break

			elif i == 3:
				print("\nSelection 3: Heat map of attribute correlation.")
				i = input("\nWould you like to pick a state or all states (a for all, enter for state) ")
				if i.lower() == 'q':
					continue
				if i.lower() == 'a':
					state = ('United States','ALL')
				else:
					state = Q.pick_state()
					while state[1] == "US" or state[1] == "DC" or state[1] == "PR":
						print("ERROR: Not enough data for state: {:}".format(state[0]))
						state = Q.pick_state()
				while True:
					print("\nWhich attributes would you like to see the correlation of for {:}:".format(state[0]))
					print("1. Demographic vs Demographic")
					print("2. Health vs Health")
					print("3. Health vs Demographic")
					i = input("Which would you like to see?(q to quit, r for random) ")
					if i.lower() == 'q':
						break
					elif i.lower() == 'r':
						i = random.randint(1,3)
					try:
						i = int(i)
					except ValueError:
						continue
					Q.self_heat_corr(i, state)

			elif i == 4:
				print("\nSelection 4: Heat map of correlation of attribute's change over time.")
				i = input("\nWould you like to pick a state or all states (a for all, enter for state) ")
				if i.lower() == 'q':
					continue
				if i.lower() == 'a':
					state = ('United States','ALL')
				else:
					state = Q.pick_state()
					while state[1] == "US" or state[1] == "DC" or state[1] == "PR":
						print("ERROR: Not enough data for state: {:}".format(state[0]))
						state = Q.pick_state()
				while True:
					print("\nWhich attributes would you like to see the correlation of change for {:}:".format(state[0]))
					print("1. Demographic vs Demographic")
					print("2. Health vs Health")
					print("3. Health vs Demographic")
					i = input("Which would you like to see?(q to quit, r for random) ")
					if i.lower() == 'q':
						break
					elif i.lower() == 'r':
						i = random.randint(1,3)
					try:
						i = int(i)
					except ValueError:
						continue
					Q.self_change_heat_corr(i, state)

			elif i == 5:
				print("\nSelection 5: Top or bottom ranked counties in a state (or US).")
				i = input("\nWould you like to pick a state or all states (a for all, enter for state) ")
				if i.lower() == 'a':
					state = ('United States','ALL')
				elif i.lower() == 'q':
					continue
				else:
					state = Q.pick_state()
					if state[1] == "US":
						state = ('United States','ALL')
					else:
						while state[1] == "DC":
							print("ERROR: Not enough counties/data in: {:}".format(state[0]))
							state = Q.pick_state()
				while True:
					i = input("\nWould you like to look at demographic or health attributes?(h for health, r for random, q to quit) ")
					if i.lower() == 'q':
						break
					elif i.lower() == 'r':
						if random.randint(1,2) == 1:
							i = 'h'
					if i.lower() == 'h':
						Q.top_n_health(state)
					else:
						Q.top_n_demo(state)
			elif i == 6:
				input("\nSelection 6: Relative ranking of any two counties in a state.  Press enter to continue: ")
				state = Q.pick_state()
				while state[1] == "US" or state[1] == "PR" or state[1] == "DC":
					print("ERROR: No ranking data for state: {:}".format(state[0]))
					state = Q.pick_state()
				while True:
					Q.two_fips_rank(state)
					i = input("\nChoose another two FIPS?(q to quit) ")
					if i.lower() == 'q':
						break
			elif i == 7:
				print("\nSelection 7: Heatmap of the US generated by plotly.")
				print("Which types of attributes would you like to explore?")
				print("1. Demographic factors.")
				print("2. Health factors.")
				print("3. Health factors by within-state quartiles.")
				print("4. Within state rankings for cumulative health measures.")
				print("5. Quartiles of within state rankings for cumulative health measures.")
				i = input("Which would you like to see?(q to quit, r for random) ")
				if i.lower() == 'q':
					continue
				if i.lower() == 'r':
					i = random.randint(1,5)
				try:
					i = int(i)
				except ValueError:
					i = random.randint(1,5)
				if i == 1:
					Q.color_map_demo()
				elif i == 2:
					Q.color_map_health("")
				elif i == 3:
					Q.color_map_health(" quartile")
				elif i == 4:
					Q.color_map_rankings("")
				elif i == 5:
					Q.color_map_rankings(" quartile")
		except Exception as e:
			print(e)
			print("ERROR: Not enough data or something really bad happened")
			print("Try another query or q to quit.")



