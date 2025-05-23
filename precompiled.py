import csv, random
import pandas as pd
import syslogger
import math
logger = syslogger.syslogger()
CHUNK_SIZE = 1000

logger.log(f"Reading compiled dataset in chunks of {CHUNK_SIZE}", True)
compiledRowsList = []
nChunk = 0
for chunk in pd.read_csv("./data/GTD.csv", chunksize = CHUNK_SIZE):
    for index, row in chunk.iterrows():
        compiledRowsList.append(row)
    nChunk += 1
    print(f"- {nChunk} read")
logger.log(f"{nChunk} chunks read")