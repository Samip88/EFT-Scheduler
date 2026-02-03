# EFT-Scheduler
Implementation of resource scheduling algorithm for ds-sim, discrete-event simulator.
Requires ds-sim to run.

To run with ds_test (tested inside podman with final.py file in main directory):

"python3 ds_test.py "python3 final.py --port 50000 " -p 50000 -n -c TestConfigs -r ./results/ref_results.json"

For individual config:

Run the server inside podman with: "./ds-server -n -p 50000 -c ./configs/ds-sample-config01.xml "
