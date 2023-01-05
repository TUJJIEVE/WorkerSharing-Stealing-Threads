1. The code includes running tests as well so compiling and running the code will run the test also
2. To compile use g++ version that is shipped with ubuntu 18.10 or greater.

3. To compile use ' g++ filename.cpp -lrt -pthread -latomic '

4. To run test ensure inp-params.txt file is present in the same folder give parameters in the input the params file.

	The input format is "num_threads num_jobs num_iterations"
	if multiple lines are provided then it executes multiple times
	note : only max 32 threads are allowed . More than that causes memory issues related to threads.

5. This generates avg.txt file which includes the times generated.
