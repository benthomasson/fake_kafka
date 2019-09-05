
VERSION=0.3

mkdir -p results-${VERSION}

for n in 10 100 1000 10000
do
./single_process_with_api.py -n ${n} --csv1 results-${VERSION}/single-process-with-api-producer.csv --csv2 results-${VERSION}/single-process-with-api-consumer.csv
done


for n in 10 100 1000 10000
do
./single_process_with_api.py -n ${n} --key test --csv1 results-${VERSION}/single-process-with-api-producer-key.csv --csv2 results-${VERSION}/single-process-with-api-consumer-key.csv
done


for n in 10 100 1000 10000
do
./single_process_with_api.py -n ${n} --partition 0 --csv1 results-${VERSION}/single-process-with-api-producer-partition.csv --csv2 results-${VERSION}/single-process-with-api-consumer-partition.csv
done

