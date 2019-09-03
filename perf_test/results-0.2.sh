
for n in 10 100 1000 10000 100000 1000000 
do
./single_process.py -n ${n} --csv1 results-0.2/single-process-producer.csv --csv2 results-0.2/single-process-consumer.csv
done


for n in 10 100 1000 10000 100000 1000000 
do
./single_process.py -n ${n} --key test --csv1 results-0.2/single-process-producer-key.csv --csv2 results-0.2/single-process-consumer-key.csv
done


for n in 10 100 1000 10000 100000 1000000 
do
./single_process.py -n ${n} --partition 0 --csv1 results-0.2/single-process-producer-partition.csv --csv2 results-0.2/single-process-consumer-partition.csv
done

