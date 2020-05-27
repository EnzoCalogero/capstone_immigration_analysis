import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

airlines_table_drop = "DROP TABLE IF EXISTS airlines"
airport_table_drop = "DROP TABLE IF EXISTS airports"
city_table_drop = "DROP TABLE IF EXISTS cities"
country_cost_table_drop = "DROP TABLE IF EXISTS country_cost"
country_age_table_drop = "DROP TABLE IF EXISTS country_age"
country_crime_table_drop = "DROP TABLE IF EXISTS country_crime"
country_health_table_drop = "DROP TABLE IF EXISTS country_health"
country_property_cost_table_drop = "DROP TABLE IF EXISTS country_property_cost"
country_pupolation_cost_table_drop = "DROP TABLE IF EXISTS country_pupolation"
country_life_quality_table_drop = "DROP TABLE IF EXISTS country_life_quality"
immigration_table_create_drop = "DROP TABLE IF EXISTS immigration"

# CREATE TABLES


immigration_table_create= ("""
CREATE TABLE immigration 
(
    cicid int4 PRIMARY KEY,
    i94yr int4,
    i94mon int4,
    
  	i94port varchar(6),
	biryear int4, 
	gender varchar(6),
    visatype varchar(6),
	airline varchar(6),
	cit_country varchar(256),
    res_country varchar(256),
    Visa_Type varchar(256),
    arrival_date TIMESTAMP
        
)
""")

airlines_table_create= ("""
CREATE TABLE airlines 
(
  	name varchar(256),
	iata varchar(10)    PRIMARY KEY,
	icao varchar(10),
	callsign varchar(256),
	country varchar(256)
)
""")

airports_table_create= ("""
CREATE TABLE airports 
(
  	airport_type varchar(256),
    name varchar(256),
    municipality varchar(256),
	IATA varchar(5) PRIMARY KEY
	)
""")

cities_table_create= ("""
CREATE TABLE cities 
(
  	city varchar(256) PRIMARY KEY,
    state varchar(256),
    median_age float4,
	total_pupolation float4,
    Foreign_born float4,
    Average_household float4
	)
""")

country_cost_table_create= ("""
CREATE TABLE country_cost 
(
  	country varchar(256) PRIMARY KEY,
    cost_living float4,
	rent_index float4,
    restourant_index float4
	)
""")

country_age_table_create= ("""
CREATE TABLE country_age 
(
  	country varchar(256) PRIMARY KEY,
    age_0_14 varchar(10),
	age_15_64 varchar(10),
    age_above_64 varchar(10)
	)
""")

country_crime_table_create= ("""
CREATE TABLE country_crime 
(
  	country varchar(256) PRIMARY KEY,
    crime_index float4,
    safety_index float4
	)
""")

country_health_table_create= ("""
CREATE TABLE country_health 
(
  	country varchar(256) PRIMARY KEY,
    health_care_index float4,
    health_care_exp_index float4
	)
""")

country_property_cost_table_create= ("""
CREATE TABLE country_property_cost 
(
  	country varchar(256) PRIMARY KEY,
    Price_To_Income_Ratio float4,
    Mortgage_As_Percentage_Income float4,
    Affordability_Index float4
	)
""")

country_pupolation_table_create= ("""
CREATE TABLE country_pupolation 
(
  	country varchar(256) PRIMARY KEY,
    area_km2 float4,
    population varchar(12),
    density varchar(12)
	)
""")

country_life_quality_table_create= ("""
CREATE TABLE country_life_quality 
(
  	country varchar(256) PRIMARY KEY,
    Quality_Life_Index float4,
    Health_Care_Index float4,
    Pollution_Index float4,
    Climate_Index float4
	)
""")

 

# QUERY LISTS

create_table_queries = [immigration_table_create,country_life_quality_table_create, country_pupolation_table_create, country_property_cost_table_create, country_health_table_create, country_crime_table_create, country_age_table_create, country_cost_table_create, cities_table_create, airlines_table_create, airports_table_create]
drop_table_queries = [immigration_table_create_drop, country_life_quality_table_drop,country_pupolation_cost_table_drop, country_property_cost_table_drop, country_health_table_drop, country_crime_table_drop, country_age_table_drop, country_cost_table_drop,city_table_drop, airlines_table_drop, airport_table_drop]


