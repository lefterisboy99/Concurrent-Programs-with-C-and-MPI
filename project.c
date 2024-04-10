#include<stdio.h>
#include <stdlib.h>
#include<pthread.h>
#include <limits.h>
#include<stdbool.h>
#include <time.h>

struct DLLNode{
    int productID;
    pthread_mutex_t lock;
    bool marked;
    struct DLLNode *next;
    struct DLLNode *prev;
};

struct LinkedList{
    struct DLLNode *head;
    struct DLLNode *tail;
};

struct HTNode{
    int productID;
    bool marked;
    pthread_mutex_t lock;
};

struct stackNode{
    int productID;
    struct stackNode* next;    
};


struct LinkedList *all_prod;
struct stackNode *stack;
int total_num_of_threads;
struct HTNode ***all_hash_tables;
pthread_barrier_t   barrier;
unsigned int next_prime;
pthread_mutex_t stack_lock;
int stack_size=0;



bool validate(struct DLLNode*pred,struct DLLNode*curr){
    if(pred->marked==false && curr->marked==false && pred->next==curr)
        return true;
    return false;
}

bool validate_HT(struct HTNode*pred){
    if(pred->marked==false)
        return true;
    return false;
}

bool validate_HT_del(struct HTNode*pred){
    if(pred->marked==false)
        return true;
    return false;
}

bool validate_del(struct DLLNode*pred,struct DLLNode*curr,struct DLLNode*next){
    if(pred->marked==false && curr->marked==false && next->marked==false && curr->next==next && pred->next==curr)
        return true;
    return false;
}

void insert(int key){
    struct DLLNode *pred;
    struct DLLNode *curr;
    //printf("called for: %d\n",key);
    while(true){
        pred=all_prod->head;
        curr=pred->next;
        while(curr->productID<key){
            pred=curr;
            curr=curr->next;
        }
        pthread_mutex_lock(&pred->lock);
        pthread_mutex_lock(&curr->lock);
        if(validate(pred,curr)==true){
            if(key==curr->productID){
                printf("WTFFF\n");
                exit(-1);
            }else{
                struct DLLNode *node=(struct DLLNode *) malloc(sizeof(struct DLLNode));
                node->marked=false;
                node->next=curr;
                node->prev=pred;
                node->productID=key;

                pred->next=node;
                curr->prev=node;
                
                pthread_mutex_unlock(&curr->lock);
                pthread_mutex_unlock(&pred->lock);
                return ;
                
                //printf("inserted: %d\n",node->productID);

            }
        }
        pthread_mutex_unlock(&curr->lock);
        pthread_mutex_unlock(&pred->lock);
        
       
    }
    return;
}

void delete(int key){
    struct DLLNode *pred;
    struct DLLNode *next;
    struct DLLNode *curr;
    //printf("called for: %d\n",key);
    while(true){
        pred=all_prod->head;
        curr=pred->next;
        next=curr->next;
        while(curr->productID<key){
            pred=curr;
            curr=next;
            next=curr->next;
        }
        pthread_mutex_lock(&pred->lock);
        pthread_mutex_lock(&curr->lock);
        pthread_mutex_lock(&next->lock);
        if(validate_del(pred,curr,next)==true){
            if(key==curr->productID){
                curr->marked=true;
                next->prev=pred;
                pred->next=next;
                
                pthread_mutex_unlock(&next->lock);
                pthread_mutex_unlock(&curr->lock);
                pthread_mutex_unlock(&pred->lock);
                return ;
            }else{
                printf("WTFFFF\n");
                exit(-1);
                return;

            }

        }
        pthread_mutex_unlock(&next->lock);
        pthread_mutex_unlock(&curr->lock);
        pthread_mutex_unlock(&pred->lock); 


    }
    
    return;
}

bool list_check(){
    struct DLLNode *f=all_prod->head->next;
    unsigned int total=0;
    unsigned int count=0;
    unsigned int square=total_num_of_threads*total_num_of_threads;
    while(f!=all_prod->tail){
        total++;
        count+=f->productID;
        f=f->next;
    }
    //printf("i am checking\n");
    if((total==square) && (count==square*(square-1)/2)){
        printf("List size check (expected: %u , found: %u)\n",square,total);
        printf("List keysum check (expected: %u , found: %u)\n",square*(square-1)/2,count);
        return false;
    }else{
        
        if(total!=square){
            printf("Error at count\n");
            printf("List size check (expected: %u , found: %u)\n",square,total);
            
        }
        if((count!=square*(square-1)/2)){
            printf("Error at total\n");
            printf("List keysum check (expected: %u , found: %u)\n",square*(square-1)/2,count);
            
        }
    }
    return true;
}

bool sec_list_check(){
    struct DLLNode *f=all_prod->head->next;
    unsigned int total=0;
    unsigned int square=total_num_of_threads*total_num_of_threads;
    while(f!=all_prod->tail){
        total++;
        
        f=f->next;
    }
    //printf("i am checking\n");
    if((total==square/3) ){
        printf("List size check (expected: %u , found: %u)\n",square/3,total);
        
        return false;
    }else{
        
        if(total!=square){
            printf("Error at count\n");
            printf("List size check (expected: %u , found: %u)\n",square/3,total);
            
        }
        
    }
    return true;
}

bool hash_check(){
    int i,j,counter;
    unsigned int total=0;
    unsigned int square=total_num_of_threads*total_num_of_threads;
    bool flag=false;
    for(i=0;i<total_num_of_threads/3;i++){
            //all_hash_tables[i]=(HTNode*)malloc((next_prime) * sizeof(HTNode));
            counter=0;
            for(j=0;j<next_prime;j++){
                // all_hash_tables[i][j]=(HTNode*)malloc(sizeof(HTNode));
                // all_hash_tables[i][j]->productID=-1;
                if(all_hash_tables[i][j]->marked==false){
                    counter++;
                    total+=all_hash_tables[i][j]->productID;
                }
            }
            if(3*total_num_of_threads!=counter){
                printf("Error at count\n");
                flag=true;
            }
            printf("HT[%d] size check (expected: %d , found: %d)\n",i,3*total_num_of_threads,counter);
            
        }
        if(total!=square*(square-1)/2){
            printf("Error at total\n");
            flag=true;
        }
        printf("HT keysum check (expected: %u , found: %u)\n",square*(square-1)/2,total);
        return flag;
}

bool sec_hash_check(){
    int i,j,counter=0;
    //unsigned int total=0;
    unsigned int square=total_num_of_threads*total_num_of_threads;
    bool flag=false;
    for(i=0;i<total_num_of_threads/3;i++){
            //all_hash_tables[i]=(HTNode*)malloc((next_prime) * sizeof(HTNode));
            counter=0;
            for(j=0;j<next_prime;j++){
                // all_hash_tables[i][j]=(HTNode*)malloc(sizeof(HTNode));
                // all_hash_tables[i][j]->productID=-1;
                if(all_hash_tables[i][j]->marked==false){
                    counter++;
                    //total+=all_hash_tables[i][j]->productID;
                }
            }
            if(2*total_num_of_threads!=counter){
                printf("Error at count\n");
                flag=true;
            }
            printf("HT[%d] size check (expected: %d , found: %d)\n",i,2*total_num_of_threads,counter);
            
        }
        /*if(total!=square*(square-1)/2){
            printf("Error at total\n");
            flag=true;
        }
        printf("HT keysum check (expected: %u , found: %u)\n",square*(square-1)/2,total);*/
        if(square/3!=stack_size){
            printf("Error at stack size\n");
            flag=true;
        }
        printf("Stack size check (expected: %d , found: %d)\n",square/3,stack_size);
        return flag;
}

int hash_func1(int j){
    
    return j%next_prime;

}

int hash_func2(int j){
    return j%7+1;
}

void HTInsert(int counter,int j){
    int consumer=counter%(total_num_of_threads/3);
    //int times_called=0;
    struct HTNode*pred;
    struct HTNode*curr;
    int position=hash_func1(j);
    int temp;
    //printf("%d %d\n",counter,position);
    /*while (true){
        //printf("iii%d %d\n",counter,position);
        pthread_mutex_lock(&all_hash_tables[consumer][position]->lock);
        if(validate_HT(all_hash_tables[consumer][position])==true){
            
                all_hash_tables[consumer][position]->productID=j;
                //printf("%d\n",j);
                all_hash_tables[consumer][position]->marked=false;
                pthread_mutex_unlock(&all_hash_tables[consumer][position]->lock);
                return;
            
                
        }else{
                
                pthread_mutex_unlock(&all_hash_tables[consumer][position]->lock);
                position=position+hash_func2(j);
                //printf("%d %d\n",times_called,j);
                //printf("%d %d\n",counter,position);
        }
        
    }*/
    curr=all_hash_tables[consumer][position];
    pthread_mutex_lock(&curr->lock);
    while(curr->marked!=true){
        if(validate_HT(curr)==true){
            if(curr->productID>j){
                    temp=curr->productID;
                    curr->productID=j;
                    j=temp;
                    //pthread_mutex_unlock(&curr->lock);
                    //HTInsert(counter,j);
                    //return;
            }

                pthread_mutex_unlock(&curr->lock);
                position=(position+hash_func2(j))%next_prime;
                curr=all_hash_tables[consumer][position];
                pthread_mutex_lock(&curr->lock);
                //printf("%d %d\n",times_called,j);
                //printf("%d %d\n",counter,position);
        }
    }
    curr->productID=j;
    curr->marked=false;
    pthread_mutex_unlock(&curr->lock);


    return;

}

bool HTDelete(int counter,int key){
    int consumer=counter%(total_num_of_threads/3);
    int times_called=0;
    int position=hash_func1(key);
    //int key;
    //printf("%d %d\n",counter,position);
    while (true){
        //printf("iii%d %d\n",counter,position);
        pthread_mutex_lock(&all_hash_tables[consumer][position]->lock);
        if(validate_HT_del(all_hash_tables[consumer][position])==true){
            if(all_hash_tables[consumer][position]->productID==key){ 
                //key=all_hash_tables[consumer][position]->productID;
                all_hash_tables[consumer][position]->marked=true;
                //printf("@@@@%d \n",all_hash_tables[consumer][position]->productID);
                pthread_mutex_unlock(&all_hash_tables[consumer][position]->lock);
                return false;
            
            }     
        }

        times_called++;
        pthread_mutex_unlock(&all_hash_tables[consumer][position]->lock);
        position=(position+hash_func2(key))%next_prime;
                
        //position=hash_func(j,times_called);
        //printf("%d %d\n",times_called,j);
        //printf("%d %d\n",counter,position);
        
        if(times_called==next_prime)
            return true;
        
    }

    return true;

}

bool TryPush(struct stackNode* data){
    struct stackNode* oldTop=stack;
    data->next=oldTop;
    pthread_mutex_lock(&stack_lock);
    if(data->next==stack){
        data->next=stack;
        stack=data;
        stack_size++;
        pthread_mutex_unlock(&stack_lock);
        return true;
    }
    pthread_mutex_unlock(&stack_lock);
    return false;

}


void push(int key){
    //printf("*******%d\n",key);
    struct stackNode* data=(struct stackNode*)malloc(sizeof(struct stackNode));
    data->productID=key;
    while(true){
        if(TryPush(data))return;
    }
    
    
}

struct stackNode* TryPop(){
    struct stackNode* oldTop=stack;
    if(oldTop->productID==-1)
        return oldTop;
    
    struct stackNode* newTop=oldTop->next;
    pthread_mutex_lock(&stack_lock);
    if(oldTop->next==newTop&&oldTop==stack){
        stack=newTop;
        stack_size--;
        pthread_mutex_unlock(&stack_lock);
        //printf("%d\n",oldTop->productID);
        return oldTop;
    }
        pthread_mutex_unlock(&stack_lock);
        return NULL;


}


int pop(){
    struct stackNode* rn;
    while(true){
        rn=TryPop();
        if(rn!=NULL)
            return rn->productID;

    }
    
}



void *myThreadFun(void *id)
{
    int i=(int *)id;
    int j;
    int counter=i;
    int bad_prod_id;
    int fixed_prod_id;
    for(j=0;j<total_num_of_threads;j++){
        insert(j*total_num_of_threads+i);
        //printf("id: %d key: %d\n",i,j*total_num_of_threads+i);
    }
    pthread_barrier_wait(&barrier);//1
    if(id==0){
        if(list_check()){
            exit(-1);
        }

    }
    pthread_barrier_wait(&barrier);//2


    for(j=total_num_of_threads*i;j<=((total_num_of_threads*i)+(total_num_of_threads-1));j++){
        delete(j);
        HTInsert(counter,j);
        counter++;
    }

    pthread_barrier_wait(&barrier);//1
    if(id==0){
        if(hash_check()){
            exit(-1);
        }
        /*for(i=0;i<total_num_of_threads/3;i++){
            //all_hash_tables[i]=(HTNode*)malloc((next_prime) * sizeof(HTNode));
            for(j=0;j<next_prime;j++){
                // all_hash_tables[i][j]=(HTNode*)malloc(sizeof(HTNode));
                // all_hash_tables[i][j]->productID=-1;
                printf(" id: %d",all_hash_tables[i][j]->productID);
            }
            printf("\n");
        }*/
    }
    pthread_barrier_wait(&barrier);//2
    counter=i;
    
     for(j=0;j<total_num_of_threads/3;j++){
         bad_prod_id=rand()%(total_num_of_threads*total_num_of_threads-1);
         while(HTDelete(counter+j,bad_prod_id)){
             bad_prod_id=rand()%(total_num_of_threads*total_num_of_threads-1);
            
         }
         //printf("$$%d %d\n",i,bad_prod_id);
        push(bad_prod_id);
        
    }
    


    pthread_barrier_wait(&barrier);//1

    if(id==0){
        /*for(i=0;i<total_num_of_threads/3;i++){
            all_hash_tables[i]=(HTNode*)malloc((next_prime) * sizeof(HTNode));
             for(j=0;j<next_prime;j++){
             all_hash_tables[i][j]=(HTNode*)malloc(sizeof(HTNode));
             all_hash_tables[i][j]->productID=-1;
                 if(all_hash_tables[i][j]->marked==false)
                 printf(" id: %d",all_hash_tables[i][j]->productID);
             }
             printf("\n");
         }*/
         if(sec_hash_check()){
             exit(-1);
         }
        
    }

    pthread_barrier_wait(&barrier);//2


    for(j=0;j<total_num_of_threads;j++){
         fixed_prod_id=pop();
         if(fixed_prod_id!=-1){
            //printf("called from %d put %d\n",i,fixed_prod_id);
            insert(fixed_prod_id);
         }
         //printf("id: %d key: %d\n",i,j*total_num_of_threads+i);
    }


    pthread_barrier_wait(&barrier);//1

    if(id==0){
        if(sec_list_check()){
             exit(-1);
        }
        
    }

    pthread_barrier_wait(&barrier);//2

    return;
}

bool isPrime(int n)
{
    // Corner cases
    if (n <= 1)  return false;
    if (n <= 3)  return true;
   
    // This is checked so that we can skip 
    // middle five numbers in below loop
    if (n%2 == 0 || n%3 == 0) return false;
   
    for (int i=5; i*i<=n; i=i+6)
        if (n%i == 0 || n%(i+2) == 0)
           return false;
   
    return true;
}

unsigned int nextPrime(int N)
{
 
    // Base case
    if (N <= 1)
        return 2;
 
    int prime = N;
    bool found = false;
 
    // Loop continuously until isPrime returns
    // true for a number greater than n
    while (!found) {
        prime++;
 
        if (isPrime(prime))
            found = true;
    }
 
    return prime;
}


void init(){
    srand(time(NULL));
    all_prod=(struct LinkedList *) malloc(sizeof(struct LinkedList));
    all_prod->head=(struct DLLNode *) malloc(sizeof(struct DLLNode));
    all_prod->head->productID=-1;
    all_prod->tail=(struct DLLNode *) malloc(sizeof(struct DLLNode));
    all_prod->tail->productID=INT_MAX;
    all_prod->head->next=all_prod->tail;
    all_prod->tail->prev=all_prod->head;
    all_prod->head->marked=false;
    all_prod->tail->marked=false;
    pthread_barrier_init(&barrier, NULL, total_num_of_threads);
    int i=0,j=0;
    next_prime=nextPrime(3*total_num_of_threads);
    all_hash_tables=(struct HTNode***)malloc((total_num_of_threads/3) * sizeof(struct HTNode**));
    for(i=0;i<total_num_of_threads/3;i++){
        all_hash_tables[i]=(struct HTNode**)malloc((next_prime) * sizeof(struct HTNode*));
        for(j=0;j<next_prime;j++){
            all_hash_tables[i][j]=(struct HTNode*)malloc(sizeof(struct HTNode));
            all_hash_tables[i][j]->marked=true;
        }
    }
    stack=(struct stackNode*)malloc(sizeof(struct stackNode));
    stack->next=NULL;
    stack->productID=-1;
    
    /*for(i=0;i<total_num_of_threads/3;i++){
        //all_hash_tables[i]=(HTNode*)malloc((next_prime) * sizeof(HTNode));
        for(j=0;j<next_prime;j++){
           // all_hash_tables[i][j]=(HTNode*)malloc(sizeof(HTNode));
           // all_hash_tables[i][j]->productID=-1;
           printf(" id: %d",all_hash_tables[i][j]->productID);
        }
        printf("\n");
    }
    printf("%d %d\n",total_num_of_threads/3,next_prime);*/

}

void main(int argc, char **argv){

    
    int i;
    int j=atoi(argv[1]);
    total_num_of_threads=j;
    init();
    pthread_t thread_id;
    
    for(i=0;i<j;i++){
        pthread_create(&thread_id, NULL, myThreadFun, (void *)i);
        
    }
    
    pthread_exit(NULL);
}