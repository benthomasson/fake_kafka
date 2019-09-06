
VERSION=0.4

mkdir -p results-${VERSION}

for n in 10 100 1000 10000
do
./single_process_with_ws.py -n ${n} --csv1 results-${VERSION}/single-process-with-ws-producer.csv --csv2 results-${VERSION}/single-process-with-ws-consumer.csv
done


for n in 10 100 1000 10000
do
./single_process_with_ws.py -n ${n} --key test --csv1 results-${VERSION}/single-process-with-ws-producer-key.csv --csv2 results-${VERSION}/single-process-with-ws-consumer-key.csv
done


for n in 10 100 1000 10000
do
./single_process_with_ws.py -n ${n} --partition 0 --csv1 results-${VERSION}/single-process-with-ws-producer-partition.csv --csv2 results-${VERSION}/single-process-with-ws-consumer-partition.csv
done

