**USA Names Analysis Project - Weather**  
  
This project will have you perform Data Analysis and processing using Apache Spark.   
The Project will use the weather dataset from https://www.ssa.gov/OACT/babynames/limits.html .  
This project will use only the data of babynames (1910 - 2017) for all the states in the United States of America.   
  
Each record in a file has the format:   
2-digit state code,   
sex (M = male or F = female),   
4-digit year of birth (starting with 1910),   
the 2-15 character name,   
and the number of occurrences of the name.    

Fields are delimited with a comma.   
Each file is sorted first on sex, then year of birth, and then on number of occurrences in descending order. When there is a tie on the number of occurrences names are listed in alphabetical order. This sorting makes it easy to determine a name's rank. The first record for each sex & year of birth has rank 1, the second record has rank 2, and so forth.  
    
**In particular, it will have you build Apache Spark that yields the following analysis:**    
1. Male, Female and Total Population of every state for the last five years.  
2. Most Common Names, Most Common Female Names and Most Common Male Names in every state.    
3. Male, Female and Total Population of entire country for the last five years.  
