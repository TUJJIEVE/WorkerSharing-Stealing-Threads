#include <iostream>
#include <atomic>
#include <thread>
#include <stdio.h>
#include <functional>
#include <vector>
#include <random>
#include <map>
#include <stdlib.h>
#include <cstdint>
#include <mutex>
#include <queue>
#include <fstream>
typedef std::function<void(int)> JOB;

// // Global variables
// /*******************************************************************************/

std::atomic<int> isDone{0};
std::map<std::thread::id,int> threadIdMap;
std::vector<std::unique_ptr<std::mutex> > mutexes;
std::vector<std::queue<JOB>> jobQueue;
//double waitTime = 0.0;
double waitTime[100] ;
double maxWaitTime[100] ;


std::fstream ifile;
std::fstream avgFile;
std::fstream ofile;



std::map<int,std::tuple<int,int,float>> statsPerThread;

int getThreadId(std::thread::id tid){
    return threadIdMap.at(tid);
}
//*******************************************************************************/
// /*  Work sharing thread uses the global jobQueue defined above. Uses global array of mutexes defined above.
//     Sharing mechanism where the probability of sharing job is (1/(s+1)) where s is the size of the thread's job queue

// */

class WorkSharingThread{
    int randomRange;
    std::default_random_engine generator;
    std::uniform_int_distribution<int> dist{std::uniform_int_distribution<int>(0,100)};
    int jobsStolen;
    int totalJobsCompleted;
    //std::mutex queueMutex;
    int threshold;
    public:
        WorkSharingThread(int max){
            //jobQueue = jobQ;
            randomRange = max;
            jobsStolen = 0;
            totalJobsCompleted = 0;
            threshold  = 20;
            for (int i=0;i<max;i++){
                mutexes.push_back(std::unique_ptr<std::mutex>(new std::mutex));
            }
        }
        void run();
        void balance(int q0,int q1);

};


// /* Balance function takes two candidate thread indexes from which exchange of jobs is to take place.


// */
void WorkSharingThread::balance(int q0,int q1){
    {   //printf("aquiring lock %d\n",q0);
        std::unique_lock<std::mutex> l(*(mutexes.at(q0))); // acquire lock on the queue0
        {   //printf("Balacing\n");
            std::unique_lock<std::mutex> l(*(mutexes.at(q1))); /// acquire lock on queue1
            int qmin = (jobQueue.at(q0).size() < jobQueue.at(q1).size()) ? q0 : q1;
            int qmax = (jobQueue.at(q0).size() < jobQueue.at(q1).size()) ? q1 : q0;
            /// If size difference is greater than some threshold
            if (jobQueue.at(qmin).size() - jobQueue.at(qmax).size()  >= threshold){
                // take jobs until same size is reached
                while (jobQueue.at(qmax).size() > jobQueue.at(qmin).size() && jobQueue.at(qmax).size() !=0){
                    //printf("poping %d\n",jobQueue.at(qmax).size());
                    jobQueue.at(qmin).push(jobQueue.at(qmax).front());
                    jobQueue.at(qmax).pop();
                    //printf("popping done\n");
                }
            }
        }
    }
    //printf("done\n");
}


// /*
//     Function to execute the jobs present in the queue
// */
void WorkSharingThread::run(){
    std::chrono::time_point<std::chrono::system_clock> beginCollect,endCollect; // for noting the times

    std::this_thread::sleep_for(std::chrono::seconds(2));    // for test purposes
    beginCollect = std::chrono::system_clock::now();         

    int threadId = getThreadId(std::this_thread::get_id());
    if (threadId == 0) return;
    //printf("Thread id %d\n",threadId);
    JOB task = NULL;
    while (true){
        if (isDone) break;

        std::unique_lock<std::mutex> l(*(mutexes.at(threadId))); // acquire the lock on the queue
        int size = jobQueue.at(threadId).size();                  // get the size
        
        if (size!=0) task = jobQueue.at(threadId).front(); // if size +ve get the t ask
        if(size!=0) jobQueue.at(threadId).pop();
        l.unlock();
        if (size!=0){
            task(1); // if task present 
        }
        l.lock();
        size = jobQueue.at(threadId).size();  // get the size
        l.unlock();
        // with probability (1/(size +1)) deciding wether to balance the queue or not 
        if ( (dist(generator)%(size+1)) == size){
            int victim = dist(generator)%randomRange;  // picking random victim
            int min = (victim <= threadId) ? victim : threadId;   // get whose queue has minimum and maximum to acquire the lock in that order
            int max = (victim <= threadId) ? threadId : victim;

            if (min!=max) balance(min,max);
        }
    }
         /// For noting down the times
    endCollect = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed_time = endCollect - beginCollect;
    waitTime[threadId] += elapsed_time.count();
    
    if (maxWaitTime[threadId] < elapsed_time.count()){
        maxWaitTime[threadId] = elapsed_time.count();
    }

}

// /******************************************************************************************/


void someJob(int i){
    int tid = getThreadId(std::this_thread::get_id());
    bool isPrime = true;
    for (int k=i-100;k<i;k++){
      for(int j = 2; j <= k / 2; ++j)
  {
      if(k % j == 0)
      {
          isPrime = false;
          break;
      }
  }
  if (isPrime);
      //printf("This is prime %d\n",i);
    }
}
void fillJobs(int numThreads,int numJobs){

    std::default_random_engine generator;
    
    std::uniform_int_distribution<int> threadDis(0,numThreads-1);
    std::uniform_int_distribution<int> paramDis(0,1000000);
    int allotedThread = -1,param = -1;
    for (int i=0;i<numJobs;i++){
        
        allotedThread = threadDis(generator);
        param = paramDis(generator);
        //printf("jobid %d %d\n",i,allotedThread);
        std::function<void(int)> job = std::bind(someJob,i);
        //jobQ[allotedThread].pushBottom(job);
        jobQueue.at(allotedThread).push(job);
        //shareQ.at(allotedThread).push(job);
    }
    return;
}

/********************************************************************************/



int main(){

    ifile.open("inp-params.txt",std::ios::in);
    ofile.open("ofile.txt",std::ios::app);
    avgFile.open("avg.txt",std::ios::app);
    int numThreads = 64;                                                                                                                                    
    int numJobs = 10000;
    int k=10;
    double maxCompletionTime =0;
    // Run until all instances are completed
    while (!ifile.eof() ){
      
        ifile >> numThreads >> numJobs >> k;
         
        for (int i=0;i<numThreads;i++){
            waitTime[i] = 0.0;
            maxWaitTime[i] = 0.0;
        }                                                                                                                                                                                           if (numThreads > 32) numThreads = 32;
        for (int i=0;i<numThreads;i++){
            jobQueue.push_back(std::queue<JOB>());
        } 
        for (int g=0;g<k;g++){

            threadIdMap.clear();
            isDone = false;
            std::vector<WorkSharingThread> shareObj;
            
            std::vector<std::thread> workerThreads;

            fillJobs(numThreads,numJobs);

            for (int i=0;i<numThreads;i++){
                std::cout<<"Jobs in queue "<<i<<": "<<jobQueue.at(i).size() << std::endl;
            }
            for (int i=0;i<numThreads;i++){
                //printf("%d\n",i);
                // stealObj.push_back(WorkStealingThread(jobQueues,numThreads));
                shareObj.push_back(WorkSharingThread(numThreads));
                workerThreads.push_back(std::thread( &WorkSharingThread::run,&(shareObj.back()) ) );

                threadIdMap.insert( std::make_pair(workerThreads.back().get_id(),i) );

            }
            int count = 0;
            std::this_thread::sleep_for(std::chrono::seconds(2));
            std::chrono::time_point<std::chrono::system_clock> beginCollect,endCollect;
            beginCollect = std::chrono::system_clock::now();
            /// For termination when all the queue sizes become zero then signal isDone    
            while(true){

                count =0;
                for (int i=0;i<numThreads;i++){
                    if (jobQueue.at(i).size() == 0) count++;
                }
                //printf("count %d\n",count);
                if (count == numThreads){
                    printf("ALL empty\n");
                    isDone.store(1);
                    for (int i=0;i<numThreads;i++){
                        workerThreads.at(i).join();
                        printf("Exiting t:%d\n",i);
                    }
                    
                    break;
                    //return 0;
                }
            }

        }
        /// After running for k iterations get the average time
        double avgTime = 0.0;
        double maxTime = 0.0;
        for (int i=1;i<numThreads;i++){
            avgTime += waitTime[i]/(double)k;
            if (maxTime < maxWaitTime[i]) maxTime = maxWaitTime[i];
        }
        avgFile << "WORK SHARING POOL : Max Completion Time for K:"<< k <<" threads:"<<numThreads<<" numJobs:" << numJobs <<" is :" << maxTime << "\n";
    
        avgFile << "WORK SHARING POOL : Average Completion Time for K:"<< k <<" threads:"<<numThreads<<" numJobs:" << numJobs <<" is :" << avgTime/(double) (numThreads-1) << "\n";
        
        avgFile << "--------------X---------------\n";
    }
    return 0;
}