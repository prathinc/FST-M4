--Load input file from HDFS--
inputFile = LOAD 'hdfs:///user/projects/episode5.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);
-- Add line number to each line in the file.
ranked = RANK inputFile;
-- Filter to read from third line as first two lines are not required.
onlyDialogues = FILTER ranked BY (rank_inputFile > 2);
-- Group by name
groupByName = GROUP onlyDialogues BY name;
-- Group names and number of lines.
names = FOREACH groupByName GENERATE $0 as name, COUNT($1) as no_of_lines;
-- Order the names in desceding order of number of lines
namesOrdered = ORDER names BY no_of_lines DESC;
-- Store the data in output file
STORE namesOrdered INTO 'hdfs:///user/projects/episode5output' USING PigStorage('\t');
