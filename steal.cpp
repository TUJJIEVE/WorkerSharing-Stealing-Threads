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

// For testing purposes
/***************************************************************/
typedef std::function<void(int)> JOB;

std::atomic<int> isDone{0};
std::map<std::thread::id,int> threadIdMap;

double waitTime[100] ;
double maxWaitTime[100] ;

std::fstream ifile,avgFile,ofile;

int getThreadId(std::thread::id tid){
    return threadIdMap.at(tid);
}



/***************************************************************/


/*  Circular Array class which is used by the unbounded dequeuer class.
    Able to resize the circular array
*/
class CircularArray{
    private:
        int logCapacity;
        JOB * jobQueue;

    public:
        CircularArray(int);
        int capacity();
        JOB getTask(int);
        void putTask(int,JOB);
        CircularArray resize(int,int);
};


CircularArray::CircularArray(int capacity){
    logCapacity = capacity;
    jobQueue  = new JOB[logCapacity];// (JOB *) malloc(sizeof(JOB) * logCapacity);
    if(jobQueue == nullptr || jobQueue == NULL) printf("*****ERROR\n");
    
}
/// Function to get the capacity
int CircularArray::capacity(){
    return 1 << logCapacity;
}
/// Function to insert task 
void CircularArray::putTask(int i,JOB task){
    jobQueue[i%logCapacity] = task;
}
/// Function to get the task
JOB CircularArray::getTask(int i){
    return jobQueue[i%logCapacity];
}
/// Function to resize 
CircularArray CircularArray::resize(int bottom,int top){
    JOB * temp =  new JOB[logCapacity + 5]; // Resizing with new capacity
    printf("resizing\n");
    if (temp == NULL || temp == nullptr) printf("not allocated\n");
    for (int i=top;i<bottom;i++){
        temp[i%(logCapacity +5)] = getTask(i);     // Copying the contents
    }

    jobQueue = temp;
    return NULL;
}



/*********************************************************************************/

/* Unbounded Deque class which uses the circular queue. This provides linearizability by using atomic load store 
    and compare and swap instructions.
*/
class UnboundedDEQ{
    public:
        UnboundedDEQ(){
            log_cap = 5000;
            top = 0;
            bottom = 0;
        }
        bool isEmpty();
        void pushBottom(JOB);
        JOB popTop();
        JOB popBottom();
        alignas(32) std::atomic<CircularArray> tasks{CircularArray(20)};  // Circular array to store the tasks
        alignas(32) std::atomic<int> bottom{0};   // bottom to get the index for insertion of task
        alignas(32) std::atomic<int> top{0};     // top pointer to get the index used by stealer threads
        int log_cap;
        
};
/// Function to check if queue is empty or not
bool UnboundedDEQ::isEmpty(){
    int locTop = top;
    int locBottom = bottom;
    return locBottom<=locTop;
}
/* Method is used to push the jobs from the other end 
*/
void UnboundedDEQ::pushBottom(JOB task){
    int oldBottom = bottom;
    int oldTop = top;
    int size = oldBottom - oldTop;
    if (size >= tasks.load().capacity() -1 ){ // If capacity reached resize the circular array
        tasks.load().resize(oldBottom,oldTop); 
    }
    tasks.load().putTask(oldBottom,task);
    bottom = oldBottom+1;
}
/*  Method used by the stealing threads to steal the jobs from the other end of the queue
    It uses compare and swap for ensuring that when there are competing threads only one of them
    will modify the top variable.
*/
JOB UnboundedDEQ::popTop(){
    int oldTop = top.load();
    int newTop = oldTop +1;
    int oldBottom = bottom;
    int size = oldBottom - oldTop;

    if (size <=0) return NULL;

    JOB task = tasks.load().getTask(oldTop);
    if (top.compare_exchange_strong(oldTop,newTop)){  // When two competing threads present then only one will change top
        return task;
    }
    return NULL;
}
/* Method is used to pop jobs . This method is used by the threads who is the owner of this queue structure
    It also uses compare and swap atomic instruction to ensure that when there is only one job in the queue 
    Only one thread either the stealer thread or the owner threads modifies the top variable.

*/
JOB UnboundedDEQ::popBottom(){
    //printf("%d Popping from bottom %d %d\n",getThreadId(std::this_thread::get_id()),bottom.load(),top.load());
    bottom-=1;
    int oldTop = top.load();
//    printf("old %d %d\n",oldTop,getThreadId(std::this_thread::get_id()));
    int newTop = oldTop+1;
//    printf("hi %d\n",getThreadId(std::this_thread::get_id()) );
    int size = bottom - oldTop;
//    printf("%d %d size\n",size,getThreadId(std::this_thread::get_id()));
    if (size <0){
        bottom = oldTop;
        return NULL;
    }
    JOB task = tasks.load().getTask(bottom);
    if (size >0){
        return task;
    }
    /// if size is 0 means only one remaining task was there in the queue.
    if (!top.compare_exchange_strong(oldTop,newTop)){ // if someone else took the only remaining job then return NULL

        task = NULL;
    }
    bottom = oldTop + 1;
    return task;
}



/**********************************************************************************************************************/

/*  This class is for the work stealing threads. Job queue pointer is given which is indexed by thread id

*/
class WorkStealingThread{

    
    UnboundedDEQ * jobQueue;
    int randomRange;
    std::default_random_engine generator;
    std::uniform_int_distribution<int> dist{std::uniform_int_distribution<int>(0,100)};
    int jobsStolen;
    int totalJobsCompleted;
    public:
        WorkStealingThread(UnboundedDEQ * jobQ, int max){
            jobQueue = jobQ;
            randomRange = max;
            jobsStolen = 0;           // to keep track of number of jobs stolen 
            totalJobsCompleted = 0;
        }
        void run(); // function to execute task in the job queue.

};

/* Function for executing the jobs present in the queue 

*/
void WorkStealingThread::run(){
    std::chrono::time_point<std::chrono::system_clock> beginCollect,endCollect;

    std::this_thread::sleep_for(std::chrono::seconds(2)); // sleeping only added for testing purposes
    beginCollect = std::chrono::system_clock::now();
    
    int threadId = getThreadId(std::this_thread::get_id());
    JOB task;
    if(threadId ==0) return;
    while (true){

        if (isDone == 1) break;
        task = jobQueue[threadId].popBottom();
        while (task == NULL){
            
            if(isDone == 1) { break;} //statsPerThread.insert(std::make_pair(threadId,std::make_tuple(totalJobsCompleted,jobsStolen,0.f)));return; }
            std::this_thread::yield();
            int victim = (dist(generator) % randomRange);
            if (!jobQueue[victim].isEmpty()){
 //               printf("Victim %d %d\n",victim,threadId);
                task = jobQueue[victim].popTop();
                if (task!=NULL) jobsStolen += 1;
            }
        }
        if (isDone == 1) break;
        task(1);
        totalJobsCompleted += 1;
    }
    endCollect = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed_time = endCollect - beginCollect;
    waitTime[threadId] += elapsed_time.count();

    if (maxWaitTime[threadId] < elapsed_time.count()){
        maxWaitTime[threadId] = elapsed_time.count();
    }

}

/********************************************************************************************************/


/*  This is the job that each threads executes finding the prime numbers 
    between i-100 to i O(n^2) 
*/
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
/*  For filling the jobs in the job queue
    randomly allocates threads jobs with random parameters
*/
void fillJobs(int numThreads,int numJobs,UnboundedDEQ * jobQ){

    std::default_random_engine generator;
    
    std::uniform_int_distribution<int> threadDis(0,numThreads-1); // for random allocation
    std::uniform_int_distribution<int> paramDis(0,10000); // for random parameter
    int allotedThread = -1,param = -1;
    for (int i=0;i<numJobs;i++){
        
        allotedThread = threadDis(generator); // alloted thread
        param = paramDis(generator);
    
        std::function<void(int)> job = std::bind(someJob,i);

        jobQ[allotedThread].pushBottom(job); // push into the queue
    
    }
    return;
}


/******************************************************************************************************/

int main(){

    ifile.open("inp-params.txt",std::ios::in);
    ofile.open("ofile.txt",std::ios::app);
    avgFile.open("avg.txt",std::ios::app);
    int numThreads = 32;int oldThread = 0;                                                                                                                                    
    int numJobs = 10000;
    int k=10;
    // Run until all instances are completed
    while (!ifile.eof()){

        ifile >> numThreads >> numJobs >> k;
        
                                                                                                                                                                                                                                                                                         if (numThreads >32) {oldThread = numThreads; numThreads = 32;}
        std::vector<WorkStealingThread> stealObj;
        std::vector<std::thread> workerThreads;

        for (int i=0;i<numThreads;i++){
            waitTime[i] = 0.0;
            maxWaitTime[i] = 0.0;
        }

        /// Run for K iterations.
        for (int g=0;g<k;g++){ 

            isDone = 0;
            UnboundedDEQ jobQueues[100];

            fillJobs(numThreads,numJobs,jobQueues);

            for (int i=0;i<numThreads;i++){
                std::cout<<"Jobs in queue"<<i<<":"<<jobQueues[i].bottom << std::endl;
            }

            workerThreads.clear();
            threadIdMap.clear();

            for (int i=0;i<numThreads;i++){
                //printf("%d\n",i);
                stealObj.push_back(WorkStealingThread(jobQueues,numThreads));
                workerThreads.push_back(std::thread( &WorkStealingThread::run,&(stealObj.back()) ) );
                threadIdMap.insert( std::make_pair(workerThreads.back().get_id(),i) );

            }

            std::this_thread::sleep_for(std::chrono::seconds(2));
            int count = 0;
            /// For termination when all the queue sizes become 0 then signal isDoen 
            while(true){

                count =0;
                for (int i=0;i<numThreads;i++){
                    if (jobQueues[i].isEmpty()) count++;
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
                
                }
            }

        }

        /// After running for k iterations get the average and maximum time taken
        double avgTime = 0.0;
        double maxTime = 0.0;
        for (int i=1;i<numThreads;i++){
            avgTime += waitTime[i]/(double)k;
            if (maxTime < maxWaitTime[i]) maxTime = maxWaitTime[i];
        }

        avgFile << "WORK STEALING POOL : Max Completion Time for K:"<< k <<" threads:"<<numThreads<<" numJobs:" << numJobs <<" is :" << maxTime << "\n";

        avgFile << "WORK STEALING POOL : Average Completion Time for K:"<< k <<" threads:"<<numThreads<<" numJobs:" << numJobs <<" is :" << avgTime/(double) (numThreads-1) << "\n";
        
        avgFile << " -----------------X--------------\n";
        

    }
    return 0;
}