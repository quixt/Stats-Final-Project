import csv, random
import pandas as pd
import syslogger
import math
from warnings import simplefilter
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

logger = syslogger.syslogger()

logger.log("Starting Program")
COMPILED_FILENAME = "compiled.csv"
DATA_FILENAMES = [
    "GDPperCapita"
]
DATA_CSV_READERS = {}
HEADER = ["country","year","success","casualties", "gdp_per_cap", "econ_growth", "unemployment", "inflation", "population", "regime_type", "corruption", "human rights", "press_freedom", "religious_establishment", "religious_attitude"]
CHUNK_SIZE = 1000

# Must remove countries that:
# - Do not exist anymore
# - Have incomplete data
# - Are not recognized as sovereign nations
ILLEGAL_COUNTRY_NAMES = [
    "West Germany (FRG)",
    "East Germany (GDR)",
    "Yugoslavia",
    "West Bank and Gaza Strip",
    "Cambodia",
    "Lebanon",
    "Zaire",
    "Czechoslovakia",
    "Kosovo",
    "South Yemen",
    "North Yemen",
    "Djibouti",
    "Falkland Islands",
    "Martinique"
]

compiledRows = []

logger.log("Compiling CSV readers", True)
# Make a dictionary of csv readers to easily access the data

logger.log("CSV readers compiled")
# Overwrite any existing file to correctly clean data
compiledDataOverwrite = open(f"./{COMPILED_FILENAME}","w",newline='')
compiledDataOverwrite.write("")
compiledDataOverwrite.close()

# Main program starts
compiledData = open(f"./{COMPILED_FILENAME}","w",newline='')
compiledDataWriter = csv.writer(compiledData)
compiledDataWriter.writerow(HEADER)

logger.log(f"Reading GTD in chunks of size {CHUNK_SIZE}", True)
nChunk = 0

def countryNameValid(country):
    if country in ILLEGAL_COUNTRY_NAMES:
        return False
    return True

for chunk in pd.read_csv("./data/GTD.csv", chunksize = CHUNK_SIZE):
    for index, row in chunk.iterrows():
        if row["alternative"] != "Private Citizens & Property" and countryNameValid(row["country_txt"]):
            if math.isnan(row["nkill"]) or math.isnan(row["nwound"]):
                casualties = 0
            else:
                casualties = int(row["nkill"]) + int(row["nwound"])
            compiledRows.append([row["country_txt"], row["iyear"], row["success"], casualties])
    nChunk += 1
    print(f"- {nChunk} read")

logger.log(f"{nChunk} chunks read")
logger.log("Reading GDP per Capita dataset", True)

gdpPerCapDF = pd.read_csv("./data/GDPperCapita.csv")

logger.log("GDP per Capita read")
logger.log("Adding GDP per Capita data to compiled data", True)

nRow = 0
errorCount= 0
gdpPerCapDict = {}
for index, row in gdpPerCapDF.iterrows():
    for i in range (1970, 2022):
        gdpPerCapDict[f"{row['Country Name']} {str(i)}"] = row[str(i)]
for compiledRow in compiledRows:
    try:
        if not math.isnan(gdpPerCapDict[f"{compiledRow[0]} {compiledRow[1]}"]):
            compiledRow.append(gdpPerCapDict[f"{compiledRow[0]} {compiledRow[1]}"])
    except:
        errorCount += 1
    nRow += 1

logger.log(f"{nRow} rows added to compiled data with {errorCount} errors")
logger.log("Reading economic growth dataset", True)

econGrowthDF = pd.read_csv("./data/economicGrowth.csv")

logger.log("Economic growth dataset read")
logger.log("Adding economic growth data to compiled data", True)

nRow = 0
errorCount = 0
econGrowthDict = {}
for index, row in econGrowthDF.iterrows():
    for i in range (1970, 2022):
        econGrowthDict[f"{row['Country Name']} {str(i)}"] = row[str(i)]
for compiledRow in compiledRows:
    try:
        if not math.isnan(econGrowthDict[f"{compiledRow[0]} {compiledRow[1]}"]):
            compiledRow.append(econGrowthDict[f"{compiledRow[0]} {compiledRow[1]}"])
    except:
        errorCount += 1
    nRow += 1

logger.log(f"{nRow} rows added to compiled data with {errorCount} errors")
logger.log("Adding unemployment data to compiled data", True)

unemploymentDF = pd.read_csv("./data/unemployment.csv")
nRow = 0
errorCount = 0
unemploymentDict = {}
for index, row in unemploymentDF.iterrows():
    for i in range (1970, 2022):
        unemploymentDict[f"{row['Country Name']} {str(i)}"] = row[str(i)]
for compiledRow in compiledRows:
    try:
        if not math.isnan(unemploymentDict[f"{compiledRow[0]} {compiledRow[1]}"]):
            compiledRow.append(unemploymentDict[f"{compiledRow[0]} {compiledRow[1]}"])
    except:
        errorCount += 1
    nRow += 1

logger.log(f"{nRow} rows added to compiled data with {errorCount} errors")
logger.log("Adding inflation data to compiled data", True)

inflationDF = pd.read_csv("./data/inflation.csv")
nRow = 0
errorCount = 0
inflationDict = {}
for index, row in inflationDF.iterrows():
    for i in range (1980, 2022):
        inflationDict[f"{row['Country Name']} {str(i)}"] = row[str(i)]
for compiledRow in compiledRows:
    try:
        if inflationDict[f"{compiledRow[0]} {compiledRow[1]}"] != "no data":
            compiledRow.append(inflationDict[f"{compiledRow[0]} {compiledRow[1]}"])
    except:
        errorCount += 1
    nRow += 1

logger.log(f"{nRow} rows added to compiled data with {errorCount} errors")
logger.log("Adding population data to compiled data", True)

populationDF = pd.read_csv("./data/population.csv")
nRow = 0
errorCount = 0
populationDict = {}
for index, row in populationDF.iterrows():
    populationDict[f"{row["Entity"]} {row["Year"]}"] = int(row["Population"])
for compiledRow in compiledRows:
    try:
        compiledRow.append(populationDict[f"{compiledRow[0]} {compiledRow[1]}"])
    except:
        errorCount += 1
    nRow += 1

logger.log(f"{nRow} rows added to compiled data with {errorCount} errors")
logger.log("Adding regime data to compiled data", True)

regimeDF = pd.read_csv("./data/regime.csv")
nRow = 0
errorCount = 0
regimeDict = {}
regimeLabelDict = {0:"Closed Autocracy", 1:"Electoral Autocracy", 2: "Electoral Democracy", 3: "Liberal Democracy"}
for index, row in regimeDF.iterrows():
    regimeDict[f"{row["Entity"]} {row["Year"]}"] = regimeLabelDict[row["Regime"]]
for compiledRow in compiledRows:
    try:
        compiledRow.append(regimeDict[f"{compiledRow[0]} {compiledRow[1]}"])
    except:
        errorCount += 1
    nRow += 1

logger.log(f"{nRow} rows added to compiled data with {errorCount} errors")
logger.log("Adding corruption data to compiled data", True)

corruptionDF = pd.read_csv("./data/corruption.csv")
nRow = 0
errorCount = 0
corruptionDict = {}
for index, row in corruptionDF.iterrows():
    corruptionDict[f"{row["Entity"]} {row["Year"]}"] = row["Corruption"]
for compiledRow in compiledRows:
    try:
        compiledRow.append(corruptionDict[f"{compiledRow[0]} {compiledRow[1]}"])
    except:
        errorCount += 1
    nRow += 1
    
logger.log(f"{nRow} rows added to compiled data with {errorCount} errors")
logger.log("Adding human rights data to compiled data", True)

hrDF = pd.read_csv("./data/humanRights.csv")
nRow = 0
errorCount = 0
hrDict = {}
for index, row in hrDF.iterrows():
    hrDict[f"{row["Entity"]} {row["Year"]}"] = row["Human Rights"]
for compiledRow in compiledRows:
    try:
        compiledRow.append(hrDict[f"{compiledRow[0]} {compiledRow[1]}"])
    except:
        errorCount += 1
    nRow += 1
    
logger.log(f"{nRow} rows added to compiled data with {errorCount} errors")
logger.log("Adding press freedom data to compiled data", True)

pressFreedomDF = pd.read_csv("./data/pressFreedom.csv")
nRow = 0
errorCount = 0
pressFreedomDict = {}
for index, row in pressFreedomDF.iterrows():
    pressFreedomDict[f"{row["Entity"]} {row["Year"]}"] = row["Press Freedom"]
for compiledRow in compiledRows:
    try:
        compiledRow.append(pressFreedomDict[f"{compiledRow[0]} {compiledRow[1]}"])
    except:
        errorCount += 1
    nRow += 1
    
logger.log(f"{nRow} rows added to compiled data with {errorCount} errors")
logger.log("Reading religion data from DTA dataset", True)

rsDF = pd.read_stata("./data/religionAndState.DTA")
nRow = 0
errorCount = 0
religiousEstablishmentDict = {}
religiousAttitudeDict = {}
cleanUpCountries = {"USA":"United States", "UK": "United Kingdom", "UAE":"United Arab Emirates", "Timor":"East Timor", "Serbia (Yugoslavia)":"Serbia", "Myanmar (Burma)":"Myanmar", "Dominical Rep.": "Dominican Republic", "Cyprus, Greek":"Cyprus","Congo-Brazzaville":"People's Republic of the Congo", "Zaire (Dem Rep Congo)":"Democratic Republic of the Congo"}
cleanUpKeys = list(cleanUpCountries.keys())
for index, row in rsDF.iterrows():
    for i in range(1990, 2015):
        if type(row[f"SAX{str(i)}"]) is str:
            if row["country"] in cleanUpKeys:
                countryName = cleanUpCountries[row["country"]]
            else:
                countryName = row["country"]
            religiousEstablishmentDict[f"{countryName} {str(i)}"] = row[f"SAX{str(i)}"]
        else:
            if row["country"] in cleanUpKeys:
                countryName = cleanUpCountries[row["country"]]
            else:
                countryName = row["country"]
            religiousEstablishmentDict[f"{countryName} {str(i)}"] = "ERROR"
        if type(row[f"SBX{str(i)}"]) is str:
            if row["country"] in cleanUpKeys:
                countryName = cleanUpCountries[row["country"]]
            else:
                countryName = row["country"]
            religiousAttitudeDict[f"{countryName} {str(i)}"] = row[f"SBX{str(i)}"]
        else:
            if row["country"] in cleanUpKeys:
                countryName = cleanUpCountries[row["country"]]
            else:
                countryName = row["country"]
            religiousAttitudeDict[f"{countryName} {str(i)}"] = "ERROR"
for compiledRow in compiledRows:
    try:
        if religiousEstablishmentDict[f"{compiledRow[0]} {compiledRow[1]}"] != "ERROR":
            compiledRow.append(religiousEstablishmentDict[f"{compiledRow[0]} {compiledRow[1]}"])
        if religiousAttitudeDict[f"{compiledRow[0]} {compiledRow[1]}"] != "ERROR":
            compiledRow.append(religiousAttitudeDict[f"{compiledRow[0]} {compiledRow[1]}"])
    except:
        errorCount += 1
    nRow+=1

logger.log(f"{nRow} rows added to compiled data with {errorCount} errors")

# Final Compilation of all data
# *****************************
logger.log("Trimming incomplete rows", True)

cleanedCompiledRows = [x for x in compiledRows if len(x) == len(HEADER)]    

logger.log(f"Trimmed {len(compiledRows)-len(cleanedCompiledRows)} rows from compiled data")
logger.log(f"Writing {len(cleanedCompiledRows)} rows to compiled CSV", True)

compiledDataWriter.writerows(cleanedCompiledRows)

logger.log("Rows written to compiled csv")
logger.log("Closing CSV readers",True)

logger.log("CSV readers closed")
logger.log("Program exit")