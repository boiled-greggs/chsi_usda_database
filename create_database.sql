/*
DROP DATABASE IF EXISTS chsi_usda_ers;
CREATE DATABASE chsi_usda_ers;

DROP USER IF EXISTS chsi;
CREATE USER chsi WITH password 'chsi';

GRANT ALL PRIVILEGES ON DATABASE chsi_usda_ers TO chsi;
*/
DROP TABLE IF EXISTS index CASCADE;
DROP TABLE IF EXISTS education;
DROP TABLE IF EXISTS unemployment;
DROP TABLE IF EXISTS population;
DROP TABLE IF EXISTS poverty;
DROP TABLE IF EXISTS health;
DROP TABLE IF EXISTS healthquartile;
DROP TABLE IF EXISTS rankings;
DROP TABLE IF EXISTS subrankings;
DROP TABLE IF EXISTS subrankingsquartiles;


CREATE TABLE index
(
  fips int,
  state varchar(63),
  county varchar(127),
  ruralurbancont int,
  PRIMARY KEY (fips)
);

CREATE TABLE education
(
  fips int,
  year int,
  highschoolenrollment int,
  hsdiplomas int,
  hsgraduationrate float,
  PRIMARY KEY (fips, year),
  FOREIGN KEY (fips)
    REFERENCES index(fips)
    ON UPDATE CASCADE
    ON DELETE SET NULL
);

CREATE TABLE unemployment
(
  fips int,
  year int,
  laborforce int,
  employed int,
  unemployed int,
  unemployedpercent float,
  PRIMARY KEY (fips, year),
  FOREIGN KEY (fips)
    REFERENCES index(fips)
    ON UPDATE CASCADE
    ON DELETE SET NULL
);

CREATE TABLE population
(
  fips int,
  year int,
  census int,
  populationestimate int,
  births int,
  deaths int,
  internationalmigrants int,
  domesticmigrants int,
  PRIMARY KEY (fips, year),
  FOREIGN KEY (fips)
    REFERENCES index(fips)
    ON UPDATE CASCADE
    ON DELETE SET NULL
);

CREATE TABLE poverty
(
  fips int,
  year int,
  povertypopulation int,
  percentpovertypop float,
  povertychildren int,
  percentpovertychildren float,
  povertyunderfive int,
  percentpovertyunderfive float,
  medianhouseholdinc float,
  singleparentpercent float,
  ginicoefficient float,
  PRIMARY KEY (fips, year),
  FOREIGN KEY (fips)
    REFERENCES index(fips)
    ON UPDATE CASCADE
    ON DELETE SET NULL
);

CREATE TABLE health
(
  fips int,
  year int,
  ypll int,
  poorhealth int,
  poorphysicaldays float,
  poormentaldays float,
  lowbirthweightpercent int,
  adultsmokingpercent int,
  adultobesitypercent int,
  bingedrinkingpercent int,
  stirate int,
  teenbirthrate int,
  uninsuredpercent int,
  pcprate int,
  preventablehospitalstays int,
  diabeticscreening int,
  violentcrimerate int,
  pollution float,
  liquorstoredensity int,
  PRIMARY KEY (fips, year),
  FOREIGN KEY (fips)
    REFERENCES index(fips)
    ON UPDATE CASCADE
    ON DELETE SET NULL
);

CREATE TABLE healthquartile
(
  fips int,
  year int,
  ypll int,
  poorhealth int,
  poorphysicaldays int,
  poormentaldays int,
  lowbirthweightpercent int,
  adultsmokingpercent int,
  adultobesitypercent int,
  bingedrinkingpercent int,
  stirate int,
  teenbirthrate int,
  uninsuredpercent int,
  pcprate int,
  preventablehospitalstays int,
  diabeticscreening int,
  violentcrimerate int,
  pollution int,
  liquorstoredensity int,
  PRIMARY KEY (fips, year),
  FOREIGN KEY (fips)
    REFERENCES index(fips)
    ON UPDATE CASCADE
    ON DELETE SET NULL
);

CREATE TABLE rankings
(
  fips int,
  year int,
  outcomes int,
  outcomesquartile int,
  factors int,
  factorsquartile int,  
  PRIMARY KEY (fips, year),
  FOREIGN KEY (fips)
    REFERENCES index(fips)
    ON UPDATE CASCADE
    ON DELETE SET NULL
);

CREATE TABLE subrankings
(
  fips int,
  year int,
  lengthoflife int,
  qualityoflife int,
  healthbehaviors int,
  clinicalcare int,
  socioeconomic int,
  physicalenvironment int,
  PRIMARY KEY (fips, year),
  FOREIGN KEY (fips)
    REFERENCES index(fips)
    ON UPDATE CASCADE
    ON DELETE SET NULL
);

CREATE TABLE subrankingsquartiles
(
  fips int,
  year int,
  lengthoflife int,
  qualityoflife int,
  healthbehaviors int,
  clinicalcare int,
  socioeconomic int,
  physicalenvironment int,
  PRIMARY KEY (fips, year),
  FOREIGN KEY (fips)
    REFERENCES index(fips)
    ON UPDATE CASCADE
    ON DELETE SET NULL
);