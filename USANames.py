from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import pyspark.sql.functions as func

if __name__ == "__main__":

    sc = SparkContext(appName="USANamesAnalysis")
    sqlContext = SQLContext(sc)
    states = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', \
    'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', \
    'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', \
    'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', \
    'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', \
    'VT', 'WA', 'WI', 'WV', 'WY']
    
	
    for state in states:
        lines = sc.textFile("dataset/%s.TXT" % state)
        parts = lines.map(lambda l: l.split(","))
        obs = parts.map(lambda p: Row(state=p[0], gender=p[1], year=p[2], name=p[3], number=p[4]))

        df = sqlContext.createDataFrame(obs)

        print("\n")
        print("###########################################")
        print("Stats for state %s" % state)
        print("###########################################")
        
        malePopulation = 0
        femalePopulation = 0
        print("\n###########################################")
        print("Male/Female Count")
        print("###########################################")
        genderCount = df.groupBy("gender","year").agg(func.sum("number")).orderBy("year","gender",ascending=False).limit(10).collect()
        for name in genderCount:
            if name.gender == "M":
                print("Male Population in state %s is %d during year %s" % (state, name["sum(number)"], name.year))
                malePopulation = name["sum(number)"]
            else:
                print("Female Population in state %s is %d during year %s" % (state, name["sum(number)"], name.year))
                femalePopulation = name["sum(number)"]
            if (malePopulation != 0 and femalePopulation !=0):
                print("Total Population in state %s is %d during year %s" % (state, malePopulation+femalePopulation, name.year))
                malePopulation = 0
                femalePopulation = 0
        print("\n###########################################")
        print("Most Common Names")
        print("###########################################")
        names = df.groupBy("name").agg(func.sum("number")).sort(func.desc("sum(number)")).limit(5).collect()
        
        counter = 1
        for name in names:
            print("Common Name %s: %s - Total Number of Occurences = %d" % (counter, name.name, name["sum(number)"]))
            counter = counter + 1
        print("\n###########################################")
        print("Common Female Names")
        print("###########################################")
        femaleNames = df.filter(df.gender=="F").groupBy("name").agg(func.sum("number")).sort(func.desc("sum(number)")).limit(5).collect()
        
        counter = 1
        for names in femaleNames:
            print("Common Female Name %s: %s - Total Number of Occurences = %d" % (counter, names.name, names["sum(number)"]))
            counter = counter + 1
        print("\n###########################################")
        print("Common Male Names")
        print("###########################################")
        maleNames = df.filter(df.gender=="M").groupBy("name").agg(func.sum("number")).sort(func.desc("sum(number)")).limit(5).collect()
        
        counter = 1
        for names in maleNames:
            print("Common Male Name %s: %s - Total Number of Occurences = %d" % (counter, names.name, names["sum(number)"]))
            counter = counter + 1
    print("\n###########################################")
    print("Entire Dataset Analysis")
    print("###########################################")
    
    lines = sc.textFile("dataset/*.TXT")
    parts = lines.map(lambda l: l.split(","))
    obs = parts.map(lambda p: Row(state=p[0], gender=p[1], year=p[2], name=p[3], number=p[4]))

    df = sqlContext.createDataFrame(obs)
    print("\n###########################################")
    print("Total Population in entire USA for last five years")
    print("###########################################")
    genderCount = df.groupBy("gender","year").agg(func.sum("number")).orderBy("year","gender",ascending=False).limit(10).collect()
    for name in genderCount:
        if name.gender == "M":
            print("Male Population is %d during year %s" % (name["sum(number)"], name.year))
            malePopulation = name["sum(number)"]
        elif name.gender == "F":
            print("Female Population is %d during year %s" % (name["sum(number)"], name.year))
            femalePopulation = name["sum(number)"]
        if (malePopulation != 0 and femalePopulation !=0):
            print("Total Population is %d during year %s" % (malePopulation+femalePopulation, name.year))
            malePopulation = 0
            femalePopulation = 0
            
    