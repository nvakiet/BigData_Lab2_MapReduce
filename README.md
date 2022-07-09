# BigData_Lab2_MapReduce
Lab Project 2 of Introduction to Big Data course. This project is for getting started with MapReduce.  
To generate data for Exercise 1, run the following command (change `-n` value for target size):  
For example, create 100mb text file:  
```
(sed -e "/'s$/d" | shuf -n 12000000 -r | fmt -w 120) < Exercise1_JavaMR/data/words-punctuations.txt > Exercise1_JavaMR/data/file1  
```
Create 200mb text file:  
```
(sed -e "/'s$/d" | shuf -n 24000000 -r | fmt -w 120) < Exercise1_JavaMR/data/words-punctuations.txt > Exercise1_JavaMR/data/file2
```
Create 500mb text file:  
```
(sed -e "/'s$/d" | shuf -n 60000000 -r | fmt -w 120) < Exercise1_JavaMR/data/words-punctuations.txt > Exercise1_JavaMR/data/file3
```
