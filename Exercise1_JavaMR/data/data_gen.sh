echo "Generating test file 100 MB"
(sed -e "/'s$/d" | shuf -n 12000000 -r | fmt -w 120) < words-punctuations.txt > file1
echo "Generating test file 200 MB"
(sed -e "/'s$/d" | shuf -n 24000000 -r | fmt -w 120) < words-punctuations.txt > file2
echo "Generating test file 500 MB"
(sed -e "/'s$/d" | shuf -n 60000000 -r | fmt -w 120) < words-punctuations.txt > file3
echo "Done!"