
VERSION=0.5

mkdir -p results-${VERSION}

for n in 10 100 1000 10000 100000 1000000 
do
./single_process.py -n ${n} --csv1 results-${VERSION}/single-process-producer.csv --csv2 results-${VERSION}/single-process-consumer.csv
done


for n in 10 100 1000 10000 100000 1000000 
do
./single_process.py -n ${n} --key test --csv1 results-${VERSION}/single-process-producer-key.csv --csv2 results-${VERSION}/single-process-consumer-key.csv
done


for n in 10 100 1000 10000 100000 1000000 
do
./single_process.py -n ${n} --partition 0 --csv1 results-${VERSION}/single-process-producer-partition.csv --csv2 results-${VERSION}/single-process-consumer-partition.csv
done

