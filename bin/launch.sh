pkill run_replica.exe
dune build bin
sleep 0.25
nohup _build/default/bin/run_replica.exe -id 0 > out/replica0.txt &
nohup _build/default/bin/run_replica.exe -id 1 > out/replica1.txt &
nohup _build/default/bin/run_replica.exe -id 2 > out/replica2.txt &
sleep 0.25
_build/default/bin/run_client.exe