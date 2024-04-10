#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#define __USE_XOPEN
#include <time.h>
#include <unistd.h>
#include <mpi.h>


#define FROM_SERVER 1
#define ACK 2
#define FROM_CLIENT 3
#define TERMINATE 4

#define START_LEADER_ELECTION_SERVERS 5
#define LEADER_ELECTION_LEADER_SERVERS 6
#define LEADER_ELECTION_ALREADY_SERVERS 7
#define LEADER_ELECTION_PARENT_SERVERS 8
#define LEADER_ELECTION_DONE_SERVERS 9
#define SERVER_LEADER 10
#define LEADER_CONFIRM 11

#define START_LEADER_ELECTION_CLIENTS 12
#define ELECT 13
#define LEADER_ELECTION_DONE_CLIENTS 14
#define I_AM_LEADER_CLIENT 15


#define REGISTER_IN 16
#define REGISTER_OUT 17
#define FORWARD 18

#define SYNC 19
#define SYNC_ACK 20

#define OVERTIME 21
#define OVERTIME_ACK 22

#define PRINT 23
#define PRINT_ACK 24

#define TOTAL_TERMINATE 25


struct List{
    int data;
    struct List *next;
};

struct DList{
    int client;
    int server;
    int type;
    long int timestamp;
    struct DList *next;
};

struct List_count{
    int id;
    int count;
    struct List_count *next;
};

struct OverTimeList{
    long int id;
    long int start_time;
    long int end_time;
    struct OverTimeList *next;
};

int deleteNode(struct List** head_ref, int key)
{
    // Store head node
    struct List *temp = *head_ref, *prev;
 
    // If head node itself holds the key to be deleted
    if (temp != NULL && temp->data == key) {
        *head_ref = temp->next; // Changed head
        //free(temp); // free old head
        return 1;
    }
 
    // Search for the key to be deleted, keep track of the
    // previous node as we need to change 'prev->next'
    while (temp != NULL && temp->data != key) {
        prev = temp;
        temp = temp->next;
    }
 
    // If key was not present in linked list
    if (temp == NULL){
        return 0;
    }
    // Unlink the node from linked list
    prev->next = temp->next;
    return 1;
    //free(temp); // Free memory
}

int deleteDNode(struct DList** head_ref, int key)
{
    // Store head node
    struct DList *temp = *head_ref, *prev;
 
    // If head node itself holds the key to be deleted
    if (temp != NULL && temp->client == key) {
        *head_ref = temp->next; // Changed head
        //free(temp); // free old head
        return 1;
    }
 
    // Search for the key to be deleted, keep track of the
    // previous node as we need to change 'prev->next'
    while (temp != NULL && temp->client != key) {
        prev = temp;
        temp = temp->next;
    }
 
    // If key was not present in linked list
    if (temp == NULL){
        return 0;
    }
    // Unlink the node from linked list
    prev->next = temp->next;
    return 1;
    //free(temp); // Free memory
}




int find_next(int id,int torus_size){

    if(id==torus_size*torus_size-torus_size+1){
        if(torus_size%2==0)
            return 1;

    }

    int row = (id-1)/torus_size+1;
    int column = id%torus_size;
    if(row%2==1){
        if(column!=0)
            return id+1;
        else
            return id+torus_size;
    }else{
        if(column!=1)
            return id-1;
        else
            return id+torus_size;
        
    }

}



int find_next_overtime(int id,int from,int torus_size){

    if(id==torus_size*torus_size&&torus_size%2==1)
        return torus_size*torus_size-torus_size+1;
    
    if(id==torus_size*torus_size-torus_size+1){
        if(torus_size%2==0)
            return 1;
        else{
            if(from==torus_size*torus_size){
                return 1;
            }
        }

    }

    int row = (id-1)/torus_size+1;
    int column = id%torus_size;
    if(row%2==1){
        if(column!=0)
            return id+1;
        else
            return id+torus_size;
    }else{
        if(column!=1)
            return id-1;
        else
            return id+torus_size;
        
    }

}



struct tm reverse_timestamp_form(long int timestamp)
{
    struct tm date;
    // number of days since Unix epoch
    // number of days since Unix epoch
    int days = timestamp / 86400;

    // calculate year
    int year = 1970;
    while (days > 365)
    {
        if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0))
        {
            days -= 366;
        }
        else
        {
            days -= 365;
        }
        year += 1;
    }
    date.tm_year = year - 0;

    // calculate day of year
    date.tm_yday = days;

    // calculate month and day of month
    int month;
    int days_in_month[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0))
    {
        days_in_month[1] = 29;
    }
    for (month = 0; month < 12; month++)
    {
        if (days < days_in_month[month])
        {
            break;
        }
        days -= days_in_month[month];
    }
    date.tm_mon = month + 1;
    date.tm_mday = days + 1;
    // calculate hour, minute, and second
    date.tm_hour = (timestamp % 86400) / 3600;
    date.tm_min = ((timestamp % 86400) % 3600) / 60;
    date.tm_sec = ((timestamp % 86400) % 3600) % 60;

    return date;
}

void main(int argc,char**argv){
    int rank, world_size, num_of_server;
    int parent=-1;
    int leader=-1;
    int client_leader=-1;
    int elect_sended=0;
    int election_flag=0;
    int register_number=0;
    int counter_for_sync_acks=0;
    int counter_for_overtime_acks=0;
    int overtime_flag=0;
    struct List*neighbors=NULL;
    struct List*childrens=NULL;
    struct List* unexplored=NULL;
    struct DList* list_for_ins=NULL;
    struct DList* list_for_atomic_ins=NULL;
    int tha_ta_spasw_ola=0;
    

    struct List* send=NULL;
    struct List* received=NULL;
    struct OverTimeList* saved_overtimes=NULL;
    

    num_of_server = atoi(argv[1]);
    int i,j,up,down,right,left;
    int counter_id=1;
    //int *topology=(int*)malloc((num_of_server*num_of_server+1) * sizeof(int));;
    int i_for_top=0;
    int j_for_top=0;
    int counter_for_top=0;
    int counter=0;
    int total_count_registers=0;
    

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Status status;

    MPI_Datatype CUSTOM_ARRAY;
    MPI_Type_contiguous(4, MPI_INT, &CUSTOM_ARRAY);
    MPI_Type_commit(&CUSTOM_ARRAY);

    MPI_Datatype CUSTOM_ARRAY_for_connect;
    MPI_Type_contiguous(2, MPI_INT, &CUSTOM_ARRAY_for_connect);
    MPI_Type_commit(&CUSTOM_ARRAY_for_connect);

    MPI_Datatype TIMESTAMP;
    MPI_Type_contiguous(8, MPI_LONG, &TIMESTAMP);
    MPI_Type_commit(&TIMESTAMP); 
    char str[80];

    sprintf(str, "%d.txt", rank);
    MPI_File handle;
    MPI_File_open(MPI_COMM_WORLD, str, MPI_MODE_CREATE | MPI_MODE_WRONLY | MPI_MODE_APPEND ,MPI_INFO_NULL, &handle);


 
    /*for(i_for_top=0;i_for_top<num_of_server;i_for_top++){
            if(i_for_top%2==0){
                printf("i am in i %d\n",i_for_top);
                for(j_for_top=i_for_top*num_of_server+1;j_for_top<i_for_top*num_of_server+1+num_of_server;j_for_top++){
                    topology[counter_for_top]=j_for_top;
                    counter_for_top++;
                    //printf("i am in 0  %d\n",j_for_top);
                }
            
            }else if(i_for_top%2==1){
                printf("i am in i %d\n",i_for_top);
                for(j_for_top=i_for_top*num_of_server+num_of_server;j_for_top>i_for_top*num_of_server;j_for_top--){
                    topology[counter_for_top]=j_for_top;
                    counter_for_top++;
                    //printf("i am in 1 %d\n",j_for_top);
                }
            
            }
            
        }
    
        topology[num_of_server*num_of_server]=-1;
        if(num_of_server%2==1){
            topology[num_of_server*num_of_server]=topology[num_of_server*num_of_server-num_of_server];
        }*/


    if(rank==0){
        int** graph;

        
        
        /*for(counter_for_top=0;counter_for_top<num_of_server*num_of_server+1;counter_for_top++){
             printf("%d ",topology[counter_for_top]);
        }*/
        graph = (int **)malloc(num_of_server* sizeof(int *));
        for(i=0;i<num_of_server;i++){
            graph[i] = (int*)malloc(num_of_server * sizeof(int));
            for(j=0;j<num_of_server;j++){
                graph[i][j] = counter_id;
                counter_id ++;
            }
        }

        /////////////////
        for(i=0;i<num_of_server;i++){
            for(j=0;j<num_of_server;j++){
                if(i==0)
                    up= graph[num_of_server - 1][j];
                else
                    up= graph[i-1][j];

                if(i==num_of_server-1)
                    down=graph[0][j];
                else
                    down=graph[i+1][j];
                if(j==0)
                    left=graph[i][num_of_server - 1];
                else
                    left=graph[i][j - 1];
                if(j==num_of_server-1)
                    right=graph[i][0];
                else
                    right=graph[i][j+1];

            
                int info_array[4];
                char a;
                info_array[3] = right;
                info_array[2] = left;
                info_array[0] = up;
                info_array[1] = down;

                
                        
                    
                //printf("sent\n");
                MPI_Send(&info_array, 1, CUSTOM_ARRAY, graph[i][j], FROM_SERVER, MPI_COMM_WORLD);
                MPI_Recv(&a,1,MPI_CHAR,MPI_ANY_SOURCE,ACK,MPI_COMM_WORLD,&status); 
                //printf("GAMA8?\n");           
            }
        } 

        
        
    

        char line[100], keyword[100], time[20], date[20];
        FILE *fptr;
        int a, b, c;
        if ((fptr = fopen(argv[2], "r")) == NULL)
        {
            printf("Error! opening file");
            exit(1);
        }
        else
        {
            while (fgets(line, sizeof(line), fptr) != NULL)
            {

                sscanf(line, "%s", keyword);
                if (strcmp(keyword, "CONNECT") == 0)
                {
                    sscanf(line, "%s %d %d", keyword, &a, &b);
                    int c[2];
                    c[0]=a;
                    c[1]=b;
                    MPI_Send(&c, 1, CUSTOM_ARRAY_for_connect, a, FROM_SERVER, MPI_COMM_WORLD);
                    MPI_Recv(&a,1,MPI_CHAR,a,ACK,MPI_COMM_WORLD,&status);
                }
                else if (strcmp(keyword, "START_LEADER_ELECTION_SERVERS") == 0)
                {
                    printf("START LEADER ELECTION SERVERS\n");
                    int i;
                    int lala[2];
                    lala[0]=-1;
                    lala[1]=-1;
                    int c[4];
                    int id;
                    c[0]=-1;
                    c[1]=-1;
                    c[2]=-1;
                    c[3]=-1;
                    for(i=num_of_server*num_of_server+1;i<world_size;i++)
                        MPI_Send(&lala, 1, CUSTOM_ARRAY_for_connect, i, TERMINATE, MPI_COMM_WORLD);
                    for(i=1;i<num_of_server*num_of_server+1;i++)
                        MPI_Send(&c, 1, CUSTOM_ARRAY, i, START_LEADER_ELECTION_SERVERS, MPI_COMM_WORLD);
                    i=1;
                    while(1){
                        MPI_Recv(&id,1,MPI_INT,MPI_ANY_SOURCE,ACK,MPI_COMM_WORLD,&status);
                        i++;
                        //printf("%d\n",i);
                        if(i==num_of_server*num_of_server){
                            for(i=1;i<num_of_server*num_of_server+1;i++)
                                MPI_Send(&c, 1, CUSTOM_ARRAY, i, START_LEADER_ELECTION_SERVERS, MPI_COMM_WORLD);
                            break;
                        }
                    }
                    MPI_Recv(&id,1,MPI_INT,MPI_ANY_SOURCE,LEADER_ELECTION_DONE_SERVERS,MPI_COMM_WORLD,&status);
                    //printf("gama8?");
                    leader=id;
                    c[0]=leader;
                    c[1]=-1;
                    c[2]=-1;
                    c[3]=-1;
                    for(i=1;i<num_of_server*num_of_server+1;i++)
                        MPI_Send(&c, 1, CUSTOM_ARRAY, i, SERVER_LEADER, MPI_COMM_WORLD);
                    i=1;
                    while(1){
                        MPI_Recv(&id,1,MPI_INT,MPI_ANY_SOURCE,LEADER_CONFIRM,MPI_COMM_WORLD,&status);
                        i++;
                        //printf("%d\n",i);
                        if(i==num_of_server*num_of_server)
                            break;
                    }
                    // printf("START_LEADER_ELECTION_SERVERS\n");
                    printf("LEADER ELECTION SERVERS DONE\n");
                }
                else if (strcmp(keyword, "START_LEADER_ELECTION_CLIENTS") == 0)
                {
                    printf("START LEADER ELECTION CLIENTS\n");
                    int i;
                    int c[4];
                    c[0]=-1;
                    c[1]=-1;
                    c[2]=-1;
                    c[3]=-1;
                    for(i=num_of_server*num_of_server+1;i<world_size;i++)
                        MPI_Send(&c, 1, CUSTOM_ARRAY, i, START_LEADER_ELECTION_CLIENTS, MPI_COMM_WORLD);
                    
                    i=num_of_server*num_of_server+1;
                    int id;
                    while(1){
                        MPI_Recv(&id,1,MPI_INT,MPI_ANY_SOURCE,ACK,MPI_COMM_WORLD,&status);
                        i++;
                        
                        if(i==world_size+1){
                            //printf("%d %d\n",i,world_size+1);
                            for(i=num_of_server*num_of_server+1;i<world_size;i++)
                                MPI_Send(&c, 1, CUSTOM_ARRAY, i, ELECT, MPI_COMM_WORLD);
                            break;
                        }
                    }
                    int tempo[4];
                    MPI_Recv(&tempo,1,CUSTOM_ARRAY,MPI_ANY_SOURCE,LEADER_ELECTION_DONE_CLIENTS,MPI_COMM_WORLD,&status);
                    client_leader=tempo[0];
                    printf("LEADER ELECTION SERVERS DONE\n");

                }
                else if (strcmp(keyword, "REGISTER") == 0)
                {
                    char type[2],string_res[100];
                    struct tm timestamps;
                    long int tmp;
                    int to;
                    sscanf(line, "%s %d %s %s %s", keyword, &to,type, time, date);
                    //printf("REGISTER: c: %d, time: %s, date: %s\n", to, time, date);

                    sprintf(string_res,"%s %s",time,date);
                    strptime(string_res, "%H:%M:%S %d/%m/%Y", &timestamps); // get the struct tm
                    //tmp = mktime(&timestamps);                              // make int time_t(long) to send it
                    /* // how to get struct tm from time_t
                    struct tm broken_time = localtime(&tmp);
                    struct tm tmp2 =broken_time;
                    printf("=>%d:%d:%d %d/%d/%d\n", tmp2.tm_hour, tmp2.tm_min, tmp2.tm_sec, tmp2.tm_mday, tmp2.tm_mon, tmp2.tm_year);*/
                    tmp = ((timestamps.tm_year - 70) * 365 + (timestamps.tm_year - 69) / 4 - (timestamps.tm_year - 1) / 100 + (timestamps.tm_year + 299) / 400 + timestamps.tm_yday) * 86400 + timestamps.tm_hour * 3600 + timestamps.tm_min * 60 + timestamps.tm_sec;
                    //printf("%ld\n",tmp);
                    long int array[8];
                    array[0] = to;
                    array[1] = to;
                    array[2] = tmp;
                    array[3] = to;
                    array[4] = 0;
                    if (strcmp(type, "IN") == 0){
                        array[5]=0;
                        MPI_Send(&array, 1, TIMESTAMP, to, REGISTER_IN, MPI_COMM_WORLD);
                        counter++;
                    }
                    if (strcmp(type, "OUT") == 0){
                        array[5]=1;
                        MPI_Send(&array, 1, TIMESTAMP, to, REGISTER_OUT, MPI_COMM_WORLD);
                        counter++;
                    }
                    total_count_registers++;

                    // printf("Hour: %d, Minute: %d, Second: %d\n", hour, minute, second);
                    //  same with date!
                }
                else if (strcmp(keyword, "SYNC") == 0)
                {
                    long int array[8];
                    struct tm tmp_timestamps;
                    //printf("SYNC STARTED\n");
                    //printf("counter is%d\n",counter);
                    while(counter!=0){
                        MPI_Recv(&array,1,TIMESTAMP,MPI_ANY_SOURCE,ACK,MPI_COMM_WORLD,&status);
                        //printf("CLIENT %d REGISTERED %d\n",array[3],array[5]);
                        //printf("%d\n",counter);
                        counter--;
                        //tmp_timestamps= reverse_timestamp_form(array[2]);
                        /*if(array[5]==1)
                            printf("CLIENT <%d> REGISTERED OUT <%d:%d:%d %d/%d/%d>\n",array[3],tmp_timestamps.tm_hour,tmp_timestamps.tm_min,tmp_timestamps.tm_sec,tmp_timestamps.tm_mday,tmp_timestamps.tm_mon+1,tmp_timestamps.tm_year);
                        else
                            printf("CLIENT <%d> REGISTERED IN <%d:%d:%d %d/%d/%d>\n",array[3],tmp_timestamps.tm_hour,tmp_timestamps.tm_min,tmp_timestamps.tm_sec,tmp_timestamps.tm_mday,tmp_timestamps.tm_mon+1,tmp_timestamps.tm_year);
                            */
                        
                    }
                    //printf("counter is%d\n",counter);
                    long int k[8];
                    //printf("SYNC\n");
                    
                    MPI_Send(&k, 1, TIMESTAMP, num_of_server*num_of_server, SYNC, MPI_COMM_WORLD);
                    MPI_Recv(&array,1,TIMESTAMP,num_of_server*num_of_server,SYNC_ACK,MPI_COMM_WORLD,&status);
                    //printf("cordinator sync ack");
                    
                    /*int num_of_sync_acks=0;
                    while(num_of_sync_acks!=num_of_server*num_of_server){
                        MPI_Recv(&array,1,TIMESTAMP,MPI_ANY_SOURCE,ACK,MPI_COMM_WORLD,&status);
                        //printf("CLIENT %d REGISTERED %d\n",array[3],array[5]);
                        //printf("%d\n",counter);
                        num_of_sync_acks++;  
                    }
                    for(i=1;i<num_of_server*num_of_server+1;i++){
                        printf("sending ack to %d\n",i);
                        MPI_Send(&k, 1, TIMESTAMP, i, ACK, MPI_COMM_WORLD);
                    }*/


                }
                else if (strcmp(keyword, "OVERTIME") == 0)
                {
                    //printf("OVERTIME START\n");
                    long int array[8];
                    int server_id;
                    sscanf(line, "%s %d %s", keyword, &server_id, date);
                    long int start_of_date;
                    long int end_of_date;
                    char *time,*time1;
                    struct tm timstamp_start;
                    struct tm timstamp_end;
                    char string_res_start[100],string_res_end[100];

                    time = "00:00:00";
                    sprintf(string_res_start, "%s %s", time, date);
                    strptime(string_res_start, "%H:%M:%S %d/%m/%Y", &timstamp_start);
                    start_of_date = ((timstamp_start.tm_year - 70) * 365 + (timstamp_start.tm_year - 69) / 4 - (timstamp_start.tm_year - 1) / 100 + (timstamp_start.tm_year + 299) / 400 + timstamp_start.tm_yday) * 86400 + timstamp_start.tm_hour * 3600 + timstamp_start.tm_min * 60 + timstamp_start.tm_sec;

                    time1 = "23:59:59";
                    sprintf(string_res_end, "%s %s", time1, date);
                    strptime(string_res_end, "%H:%M:%S %d/%m/%Y", &timstamp_end);
                    end_of_date = ((timstamp_end.tm_year - 70) * 365 + (timstamp_end.tm_year - 69) / 4 - (timstamp_end.tm_year - 1) / 100 + (timstamp_end.tm_year + 299) / 400 + timstamp_end.tm_yday) * 86400 + timstamp_end.tm_hour * 3600 + timstamp_end.tm_min * 60 + timstamp_end.tm_sec;
                    //printf("dates %ld %ld\n",start_of_date,end_of_date);
                    array[0]=start_of_date;
                    array[1]=end_of_date;
                    array[2]=0;
                    array[3]=total_count_registers;
                    array[4]=server_id;
                    MPI_Send(&array, 1, TIMESTAMP, server_id, OVERTIME, MPI_COMM_WORLD);
                    MPI_Recv(&array,1,TIMESTAMP,server_id,OVERTIME_ACK,MPI_COMM_WORLD,&status);
                    //printf("OVERTIME DONE\n");
                    //printf("OVERTIME: c: %d, date: %s\n", server_id, date);
                }
                else if (strcmp(keyword, "PRINT") == 0)
                {
                    //printf("PRINT START\n");
                    //MPI_File_open(MPI_COMM_WORLD, "file.tmp", MPI_MODE_CREATE | MPI_MODE_WRONLY | MPI_MODE_APPEND, MPI_INFO_NULL, &handle);
                    long int array[8];
                    MPI_Send(&array, 1, TIMESTAMP, num_of_server*num_of_server, PRINT, MPI_COMM_WORLD);
                    MPI_Recv(&array,1,TIMESTAMP,num_of_server*num_of_server,PRINT_ACK,MPI_COMM_WORLD,&status);
                    //printf("PRINT DONE\n");
                    //printf("PRINT START CLIENTS\n");
                    MPI_Send(&array, 1, TIMESTAMP, client_leader, PRINT, MPI_COMM_WORLD);
                    MPI_Recv(&array,1,TIMESTAMP,client_leader,PRINT_ACK,MPI_COMM_WORLD,&status);
                    //printf("PRINT DONE\n");
                    //MPI_File_close(&handle);
                }
            }

            int den_tin_palevo=1;
            long int dummy_array[8];
            for(den_tin_palevo=1;den_tin_palevo<world_size;den_tin_palevo++){
                MPI_Send(&dummy_array, 1, TIMESTAMP, den_tin_palevo, TOTAL_TERMINATE, MPI_COMM_WORLD);
            }


        }



    }else if(rank<num_of_server*num_of_server+1){       //servers





        int info_array[4];
        MPI_Recv(&info_array, 1, CUSTOM_ARRAY, 0, FROM_SERVER, MPI_COMM_WORLD, &status);
        right = info_array[3];
        left = info_array[2];
        up = info_array[0];
        down = info_array[1];
        //printf("RANKING ME IS = %d\n", rank);
        struct List*lala=(struct List*) malloc(sizeof(struct List));
        lala->data=up;
        lala->next=unexplored;
        unexplored=lala;

        lala=(struct List*) malloc(sizeof(struct List));
        lala->data=down;
        lala->next=unexplored;
        unexplored=lala;

        lala=(struct List*) malloc(sizeof(struct List));
        lala->data=left;
        lala->next=unexplored;
        unexplored=lala;

        lala=(struct List*) malloc(sizeof(struct List));
        lala->data=right;
        lala->next=unexplored;
        unexplored=lala;
        //printf("RANKING ME IS = %d MY NEIGHBORS ARE = %d-%d-%d-%d-\n", rank,right, left, up, down);
        char temp='a';
        MPI_Send(&temp,1,MPI_CHAR,0,ACK,MPI_COMM_WORLD);

        while(1){
            int c[4];
            MPI_Recv(&c, 1, CUSTOM_ARRAY, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
            if(status.MPI_TAG==START_LEADER_ELECTION_SERVERS){
                // int thrash=0;
                // MPI_Send(&thrash,1,MPI_INT,0,ACK,MPI_COMM_WORLD);
                if(parent==-1){
                    leader=rank;
                    parent=rank;
                    
                    
                    int thrash=0;
                    MPI_Send(&thrash,1,MPI_INT,0,ACK,MPI_COMM_WORLD);
                    MPI_Recv(&c, 1, CUSTOM_ARRAY, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
                    //printf("my fcking rank:%d\n",rank);
                    if(unexplored!=NULL){
                        struct List* pk=unexplored;
                        unexplored=unexplored->next;
                        int temp[4];
                        temp[0]=rank;
                        temp[1]=leader;
                        temp[2]=-1;
                        temp[3]=-1;
                        while(deleteNode(&unexplored, pk->data));
                        MPI_Send(&temp,1,CUSTOM_ARRAY,pk->data,LEADER_ELECTION_LEADER_SERVERS,MPI_COMM_WORLD);
                    }else{
                        if(parent!=rank){
                            int temp[4];
                            temp[0]=rank;
                            temp[1]=leader;
                            temp[2]=-1;
                            temp[3]=-1;
                            MPI_Send(&temp,1,CUSTOM_ARRAY,parent,LEADER_ELECTION_PARENT_SERVERS,MPI_COMM_WORLD);
                        }else{
                            //printf("I AM ROOT %d\n",rank);
                            MPI_Send(&rank,1,MPI_INT,0,LEADER_ELECTION_DONE_SERVERS,MPI_COMM_WORLD);
                        }
                    }
                }
            }else if(status.MPI_TAG==LEADER_ELECTION_LEADER_SERVERS){//c0 pj c1 new_id 
                if(leader<c[1]){
                    leader=c[1];
                    parent=c[0];
                    childrens=NULL;

                    while(deleteNode(&unexplored, c[0]));
                                    

                    if(unexplored!=NULL){
                        struct List* pk=unexplored;
                        unexplored=unexplored->next;
                        int temp[4];
                        temp[0]=rank;
                        temp[1]=leader;
                        temp[2]=-1;
                        temp[3]=-1;
                        while(deleteNode(&unexplored, pk->data));
                        MPI_Send(&temp,1,CUSTOM_ARRAY,pk->data,LEADER_ELECTION_LEADER_SERVERS,MPI_COMM_WORLD);
                    }else{
                        if(parent!=rank){
                            int temp[4];
                            temp[0]=rank;
                            temp[1]=leader;
                            temp[2]=-1;
                            temp[3]=-1;
                            MPI_Send(&temp,1,CUSTOM_ARRAY,parent,LEADER_ELECTION_PARENT_SERVERS,MPI_COMM_WORLD);
                        }else{
                            //printf("I AM ROOT %d\n",rank);
                            MPI_Send(&rank,1,MPI_INT,0,LEADER_ELECTION_DONE_SERVERS,MPI_COMM_WORLD);
                        }
                    }
                }else if(leader==c[1]){
                    int temp[4];
                    temp[0]=rank;
                    temp[1]=leader;
                    temp[2]=-1;
                    temp[3]=-1;
                    MPI_Send(&temp,1,CUSTOM_ARRAY,c[0],LEADER_ELECTION_ALREADY_SERVERS,MPI_COMM_WORLD);
                }
            }else if(status.MPI_TAG==LEADER_ELECTION_ALREADY_SERVERS){
                if(c[1]==leader){

                    if(unexplored!=NULL){
                        struct List* pk=unexplored;
                        unexplored=unexplored->next;
                        int temp[4];
                        temp[0]=rank;
                        temp[1]=leader;
                        temp[2]=-1;
                        temp[3]=-1;
                        while(deleteNode(&unexplored, pk->data));
                        MPI_Send(&temp,1,CUSTOM_ARRAY,pk->data,LEADER_ELECTION_LEADER_SERVERS,MPI_COMM_WORLD);
                    }else{
                        if(parent!=rank){
                            int temp[4];
                            temp[0]=rank;
                            temp[1]=leader;
                            temp[2]=-1;
                            temp[3]=-1;
                            MPI_Send(&temp,1,CUSTOM_ARRAY,parent,LEADER_ELECTION_PARENT_SERVERS,MPI_COMM_WORLD);
                        }else{
                            //printf("I AM ROOT %d\n",rank);
                            MPI_Send(&rank,1,MPI_INT,0,LEADER_ELECTION_DONE_SERVERS,MPI_COMM_WORLD);
                        }
                    }
                }
            }else if(status.MPI_TAG==LEADER_ELECTION_PARENT_SERVERS){//c0 children c1 new_id
                if(c[1]==leader){
                    struct List*lala=(struct List*) malloc(sizeof(struct List));
                    lala->data=c[0];
                    lala->next=childrens;
                    childrens=lala;
                    
                    if(unexplored!=NULL){
                        struct List* pk=unexplored;
                        unexplored=unexplored->next;
                        int temp[4];
                        temp[0]=rank;
                        temp[1]=leader;
                        temp[2]=-1;
                        temp[3]=-1;
                        while(deleteNode(&unexplored, pk->data));
                        MPI_Send(&temp,1,CUSTOM_ARRAY,pk->data,LEADER_ELECTION_LEADER_SERVERS,MPI_COMM_WORLD);
                    }else{
                        if(parent!=rank){
                            int temp[4];
                            temp[0]=rank;
                            temp[1]=leader;
                            temp[2]=-1;
                            temp[3]=-1;
                            MPI_Send(&temp,1,CUSTOM_ARRAY,parent,LEADER_ELECTION_PARENT_SERVERS,MPI_COMM_WORLD);
                        }else{
                            //printf("I AM ROOT %d\n",rank);
                            MPI_Send(&rank,1,MPI_INT,0,LEADER_ELECTION_DONE_SERVERS,MPI_COMM_WORLD);
                        }
                    }
                }
            }else if(status.MPI_TAG==SERVER_LEADER){
                leader=c[0];
                //printf("rank:%d leader:%d\n",rank,leader);
                MPI_Send(&leader,1,MPI_INT,0,LEADER_CONFIRM,MPI_COMM_WORLD);
                break;
            }

        }
        //c0 leader client
        //c1 to server
        //c2 timestamp
        //c3 to client
        //c4 pos
        //c5 in or out

        while(1){
            long int c[8];
            //printf("w8ing 4 next%d\n",rank);
            MPI_Recv(&c, 1, TIMESTAMP, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
            //printf("%d\n",rank);
            if(status.MPI_TAG==REGISTER_IN){
                //printf("inside register in %d\n",rank);
                if(c[1]==rank){
                    //printf("i am %d sent ack to %d\n",rank,c[3]);
                    struct DList *pk=(struct DList*) malloc(sizeof(struct DList));
                    pk->client=c[3];
                    pk->server=rank;
                    pk->type=0;
                    pk->timestamp=c[2];
                    //c[1]=s;
                    pk->next=list_for_atomic_ins;
                    list_for_atomic_ins=pk;
                    //printf("i am %d i saved in %d from %d client\n",rank,c[2],c[3]);
                    MPI_Send(&c,1,TIMESTAMP,c[3],ACK,MPI_COMM_WORLD);
                }else{
                    int next_in_torus=find_next(rank,num_of_server);
                    //printf("i am %d next in torus is %d\n",rank,next_in_torus);
                    MPI_Send(&c,1,TIMESTAMP,next_in_torus,REGISTER_IN,MPI_COMM_WORLD);
                }

            }else if(status.MPI_TAG==REGISTER_OUT){
                //printf("inside register out %d\n",rank);
                if(c[1]==rank){
                    //printf("i am %d sent ack to %d\n",rank,c[3]);
                    struct DList *pk=(struct DList*) malloc(sizeof(struct DList));
                    pk->client=c[3];
                    pk->server=rank;
                    pk->type=1;
                    pk->timestamp=c[2];
                    c[1]=rank;
                    pk->next=list_for_atomic_ins;
                    list_for_atomic_ins=pk;
                    //printf("i am %d i saved out %d from %d client\n",rank,c[2],c[3]);
                    MPI_Send(&c,1,TIMESTAMP,c[3],ACK,MPI_COMM_WORLD);
                }else{
                    int next_in_torus=find_next(rank,num_of_server);
                    //printf("i am %d next in torus is %d\n",rank,next_in_torus);
                    MPI_Send(&c,1,TIMESTAMP,next_in_torus,REGISTER_OUT,MPI_COMM_WORLD);
                }
            }else if(status.MPI_TAG==FORWARD){
                //printf("inside forward %d\n",rank);
                if(leader==rank){
                    int s=-1;
                    if(c[5]==0){
                        struct DList *pk=(struct DList*) malloc(sizeof(struct DList));
                        pk->client=c[3];
                        
                        s=register_number%(num_of_server*num_of_server)+1;
                        register_number++;
                        pk->server=s;
                        pk->type=0;
                        pk->timestamp=c[2];
                        c[1]=s;
                        pk->next=list_for_ins;
                        list_for_ins=pk;
                        //printf("CLIENT %d req IN from SERVER %d\n",c[3],c[1]);
                    }else{

                        //struct DList *pk=(struct DList*) malloc(sizeof(struct DList));
                        //pk->client=c[3];
                        struct DList *lala=list_for_ins;
                        while(lala!=NULL){
                            if(lala->type==0&&lala->client==c[3]){
                                s=lala->server;
                            }
                            lala=lala->next;
                        }
                        
                        //pk->server=s;
                        //pk->type=c[5];
                        //pk->timestamp=c[2];
                        c[1]=s;
                        //pk->next=list_for_ins;
                        //list_for_ins=pk;

                        while(deleteDNode(&list_for_ins, c[3]));
                        //printf("CLIENT %d req OUT from SERVER %d\n",c[3],c[1]);

                    }

                    if(s==rank){
                        //printf("i am %d sent ack to %d\n",rank,c[3]);
                        struct DList *pk=(struct DList*) malloc(sizeof(struct DList));
                        pk->client=c[3];
                        pk->server=rank;
                        pk->type=c[5];
                        pk->timestamp=c[2];
                        //c[1]=s;
                        pk->next=list_for_atomic_ins;
                        list_for_atomic_ins=pk;
                        MPI_Send(&c,1,TIMESTAMP,c[3],ACK,MPI_COMM_WORLD);
                    }else{
                        if(num_of_server%2==1){
                            //printf("i am %d next in torus is %d\n",rank,num_of_server*num_of_server-num_of_server+1);
                            MPI_Send(&c,1,TIMESTAMP,num_of_server*num_of_server-num_of_server+1,FORWARD,MPI_COMM_WORLD);
                        }else{
                            int next_in_torus=find_next(rank,num_of_server);
                            //printf("i am %d next in torus is %d\n",rank,next_in_torus);
                            if(c[5]==0)
                                MPI_Send(&c,1,TIMESTAMP,next_in_torus,REGISTER_IN,MPI_COMM_WORLD);  
                            else 
                                MPI_Send(&c,1,TIMESTAMP,next_in_torus,REGISTER_OUT,MPI_COMM_WORLD);  
                        }
                    }
                }else{
                    if(c[1]==rank){
                        //printf("i am %d sent ack to %d\n",rank,c[3]);
                        struct DList *pk=(struct DList*) malloc(sizeof(struct DList));
                        pk->client=c[3];
                        pk->server=rank;
                        pk->type=c[5];
                        pk->timestamp=c[2];
                        //c[1]=s;
                        pk->next=list_for_atomic_ins;
                        list_for_atomic_ins=pk;
                        MPI_Send(&c,1,TIMESTAMP,c[3],ACK,MPI_COMM_WORLD);
                    }else{
                        //int next_in_torus=find_next(rank,num_of_server);
                        //printf("i am %d next in torus is %d\n",rank,1);
                        if(c[5]==0)
                                MPI_Send(&c,1,TIMESTAMP,1,REGISTER_IN,MPI_COMM_WORLD);  
                            else 
                                MPI_Send(&c,1,TIMESTAMP,1,REGISTER_OUT,MPI_COMM_WORLD); 
                    }
                }
            }else if(status.MPI_TAG==SYNC){
                //printf("got sync %d\n",rank);
                long int total_count=0;
                long int total_timestamp=0;
                //MPI_Send(&c,1,TIMESTAMP,0,ACK,MPI_COMM_WORLD);
                //MPI_Recv(&c, 1, TIMESTAMP, 0, ACK, MPI_COMM_WORLD, &status);
                struct List *tmp_childrens=childrens;
                struct DList *tmp_atomic_ins=list_for_atomic_ins;
                int all_ins=0,all_ins_a=0;
                int num_of_ins=0,num_of_outs=0;
                // while(tmp_atomic_ins!=NULL){
                //     if(tmp_atomic_ins->type==0) printf("I AM SERVER %d AND HAVE %d FOR CLIENT %d TIMESTAMP %d\n",rank,tmp_atomic_ins->type,tmp_atomic_ins->client,tmp_atomic_ins->timestamp);
                //     else printf("I AM SERVER %d AND HAVE %d FOR CLIENT %d TIMESTAMP %d\n",rank,tmp_atomic_ins->type,tmp_atomic_ins->client,tmp_atomic_ins->timestamp);
                //     tmp_atomic_ins=tmp_atomic_ins->next;
                // }

                tmp_atomic_ins=list_for_atomic_ins;
                while(tmp_atomic_ins!=NULL){
                    all_ins++;
                    struct DList *tmp_atomic_ins2=list_for_atomic_ins;
                    int flag=-1;
                    while(tmp_atomic_ins2!=NULL){
                        flag=-1;
                        if(tmp_atomic_ins->client==tmp_atomic_ins2->client&&tmp_atomic_ins->type!=tmp_atomic_ins2->type ){
                            if(tmp_atomic_ins2->type==1){
                                if(tmp_atomic_ins->timestamp>tmp_atomic_ins2->timestamp){
                                    //printf("WHAT THE FCKKKKKKKKK\n");
                                    flag=0;
                                }
                            }else{
                                if(tmp_atomic_ins->timestamp<tmp_atomic_ins2->timestamp){
                                    //printf("WHAT THE FCKKKKKKKKK\n");
                                    flag=0;
                                }

                            }
                        }
                        
                        // if(flag==-1){
                        //     printf("FCKKKK ME\n");
                        // }
                        tmp_atomic_ins2=tmp_atomic_ins2->next;
                    }
                    if(tmp_atomic_ins->type==0) num_of_ins+=tmp_atomic_ins->timestamp;
                    else    num_of_outs+=tmp_atomic_ins->timestamp;
                    tmp_atomic_ins=tmp_atomic_ins->next;
                }
                
                while(tmp_childrens!=NULL){
                    MPI_Send(&c,1,TIMESTAMP,tmp_childrens->data,SYNC,MPI_COMM_WORLD);
                    //printf("SENDING TO %d\n",tmp_childrens->data);
                    tmp_childrens=tmp_childrens->next;
                }
                
                if(rank!=parent){
                    c[6]=all_ins;
                    c[7]=num_of_outs-num_of_ins;
                    //printf("outs - ins %d\n",c[7]);
                    //printf("MY RANK IS %d MY PARENT IS %d\n",rank,parent);
                    //printf("MY NUMBER OF INST IS %d and my rank is %d\n",all_ins,rank);
                    MPI_Send(&c,1,TIMESTAMP,parent,SYNC_ACK,MPI_COMM_WORLD);
                }
                else{
                    //printf("I AM LEADER I HAVE %d requests\n",all_ins);
                    total_timestamp=num_of_outs-num_of_ins;
                    total_count=total_count+all_ins;
                    //struct DList *tmp_for_ins=list_for_ins;
                    // while(tmp_for_ins!=NULL){
                    //     all_ins_a++;
                    //     tmp_for_ins=tmp_for_ins->next;
                    // }
                    // printf("ALL INSTTRR%d \n",all_ins_a);
                }


                
                
                while(1){
                    long int cc[8];
                    MPI_Recv(&cc, 1, TIMESTAMP, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
                    if(status.MPI_TAG==SYNC_ACK){
                        if(rank!=parent){
                            MPI_Send(&cc,1,TIMESTAMP,parent,SYNC_ACK,MPI_COMM_WORLD);
                        }else{
                            //printf("I AM LEADER I RECEIVED %d requests from%d\n",cc[6],cc[7]);
                            counter_for_sync_acks++;
                            total_count+=cc[6];
                            total_timestamp+=cc[7];
                            if(counter_for_sync_acks==num_of_server*num_of_server-1){
                                //printf("TOTAL NUMBER OF REG %d TOTAL TIMESTAMPS %d\n",total_count,total_timestamp);
                                tmp_childrens=childrens;
                                while(tmp_childrens!=NULL){
                                    MPI_Send(&c,1,TIMESTAMP,tmp_childrens->data,TERMINATE,MPI_COMM_WORLD);
                                    //printf("SENDING TO %d\n",tmp_childrens->data);
                                    tmp_childrens=tmp_childrens->next;
                                }
                                while(counter_for_sync_acks!=0){
                                    MPI_Recv(&cc, 1, TIMESTAMP, MPI_ANY_SOURCE, ACK, MPI_COMM_WORLD, &status);
                                    //printf("SENDING TO %d\n",tmp_childrens->data);
                                    counter_for_sync_acks--;
                                }
                                printf("LEADER SYNC OK\n");
                                MPI_Send(&c,1,TIMESTAMP,0,SYNC_ACK,MPI_COMM_WORLD);

                                break;

                            }
                        }
                        
                    }else if(status.MPI_TAG==TERMINATE) {
                        tmp_childrens=childrens;
                        while(tmp_childrens!=NULL){
                                    MPI_Send(&c,1,TIMESTAMP,tmp_childrens->data,TERMINATE,MPI_COMM_WORLD);
                                    //printf("SENDING TO %d\n",tmp_childrens->data);
                                    tmp_childrens=tmp_childrens->next;
                        }
                        MPI_Send(&c,1,TIMESTAMP,num_of_server*num_of_server,ACK,MPI_COMM_WORLD);

                        break;
                    }
                }



                // c[0]=start_of_date;
                // c[1]=end_of_date;
                // c[2]=from;
                // c[3]=total_count_registers;
                // c[4]=overtime leader;
                //c5 start
                //c6 end
                //c7 id

            }else if(status.MPI_TAG==OVERTIME){
                long int tmp[8];
                //MPI_Send(&tmp,1,TIMESTAMP,find_next_overtime(rank,c[2],num_of_server),OVERTIME,MPI_COMM_WORLD);
                if(c[4]==rank){

                    if(c[2]!=num_of_server*num_of_server&&overtime_flag==0){
                        tmp[0]=c[0];
                        tmp[1]=c[1];
                        tmp[2]=rank;
                        tmp[3]=c[3];
                        tmp[4]=c[4];
                        tmp[5]=rank;
                        tmp[6]=rank;
                        tmp[7]=0;
                        //printf("I AM %d SENT TORUS NEXT %d\n",rank,find_next_overtime(rank,c[2],num_of_server));
                        MPI_Send(&tmp,1,TIMESTAMP,find_next_overtime(rank,c[2],num_of_server),OVERTIME,MPI_COMM_WORLD);
                        overtime_flag=1;
                    }else{

                        if(c[2]==num_of_server*num_of_server){

                            if(c[2]==num_of_server*num_of_server&&overtime_flag==1){
                                tmp[0]=c[0];
                                tmp[1]=c[1];
                                tmp[2]=rank;
                                tmp[3]=c[3];
                                tmp[4]=c[4];
                                tmp[5]=rank;
                                tmp[6]=rank;
                                tmp[7]=0;
                                //printf("I AM %d SENT TORUS NEXT 1\n",rank);
                                MPI_Send(&tmp,1,TIMESTAMP,1,OVERTIME,MPI_COMM_WORLD);
                                overtime_flag=2;
                            }
                        }

                    }

                }else{
                    tmp[0]=c[0];
                    tmp[1]=c[1];
                    tmp[2]=rank;
                    tmp[3]=c[3];
                    tmp[4]=c[4];
                    tmp[5]=rank;
                    tmp[6]=rank;
                    tmp[7]=0;
                    //printf("I AM %d SENT TORUS NEXT %d\n",rank,find_next_overtime(rank,c[2],num_of_server));
                    MPI_Send(&tmp,1,TIMESTAMP,find_next_overtime(rank,c[2],num_of_server),OVERTIME,MPI_COMM_WORLD);
                    struct DList *tmp_atomic_ins=list_for_atomic_ins;
                    //int tha_ta_spasw_ola=0;
                    if(rank!=num_of_server*num_of_server-num_of_server+1 || num_of_server%2==0){
                            while(tmp_atomic_ins!=NULL){
                                if(tmp_atomic_ins->type!=0){
                                    tmp[0]=c[0];
                                    tmp[1]=c[1];
                                    tmp[2]=rank;
                                    tmp[3]=c[3];
                                    tmp[4]=c[4];
                                    tmp[5]=-1;   //in case of printing -1 something is terrible wrong with my life and all started at 2017 when i passed at CSD
                                    tmp[6]=tmp_atomic_ins->timestamp;
                                    tmp[7]=rank;
                                    struct tm a= reverse_timestamp_form(tmp_atomic_ins->timestamp);
                                    
                                    struct DList *tmp_atomic_ins2=list_for_atomic_ins;
                                    while(tmp_atomic_ins2!=NULL){
                                        struct tm b= reverse_timestamp_form(tmp_atomic_ins2->timestamp);
                                        if(tmp_atomic_ins2->client==tmp_atomic_ins->client&&tmp_atomic_ins2->type!=tmp_atomic_ins->type&&tmp_atomic_ins2->timestamp<tmp_atomic_ins->timestamp&&a.tm_mday==b.tm_mday&&a.tm_mon==b.tm_mon&&a.tm_year==b.tm_year){
                                            tmp[5]=tmp_atomic_ins2->timestamp;
                                            //printf("I AM %d SENT ACK1 for client %d time_start <%d:%d:%d %d/%d/%d> time_end <%d:%d:%d %d/%d/%d>\n",rank,tmp[7],a.tm_hour, a.tm_min, a.tm_sec, a.tm_mday, a.tm_mon, a.tm_year,b.tm_hour, b.tm_min, b.tm_sec, b.tm_mday, b.tm_mon, b.tm_year);

                                        }
                                        

                                        //printf("I AM %d SENT ACK1 for client %d time_start <%d:%d:%d %d/%d/%d> time_end <%d:%d:%d %d/%d/%d>\n",rank,tmp[7],a.tm_hour, a.tm_min, a.tm_sec, a.tm_mday, a.tm_mon, a.tm_year,b.tm_hour, b.tm_min, b.tm_sec, b.tm_mday, b.tm_mon, b.tm_year);
                                        
                                        
                                        tmp_atomic_ins2=tmp_atomic_ins2->next;
                                    }
                                    
                                    //printf("I AM %d SENT ACK1 for client %d time_start <%d:%d:%d %d/%d/%d> time_end <%d:%d:%d %d/%d/%d>\n",rank,tmp[7],a.tm_hour, a.tm_min, a.tm_sec, a.tm_mday, a.tm_mon, a.tm_year,b.tm_hour, b.tm_min, b.tm_sec, b.tm_mday, b.tm_mon, b.tm_year);
                                    MPI_Send(&tmp,1,TIMESTAMP,find_next_overtime(rank,c[2],num_of_server),OVERTIME_ACK,MPI_COMM_WORLD);
                                    
                                }
                                tmp_atomic_ins=tmp_atomic_ins->next;
                            }
                        //printf("I AM %d AND I SENT THA TA SPASW OLA %d\n",rank,tha_ta_spasw_ola);
                    }else {
                        if(rank==num_of_server*num_of_server-num_of_server+1 && tha_ta_spasw_ola==0){
                                tha_ta_spasw_ola=1;
                                while(tmp_atomic_ins!=NULL){
                                    if(tmp_atomic_ins->type!=0){
                                        tmp[0]=c[0];
                                        tmp[1]=c[1];
                                        tmp[2]=rank;
                                        tmp[3]=c[3];
                                        tmp[4]=c[4];
                                        tmp[5]=-1;   //in case of printing -1 something is terrible wrong with my life and all started at 2017 when i passed at CSD
                                        tmp[6]=tmp_atomic_ins->timestamp;
                                        tmp[7]=rank;

                                        struct DList *tmp_atomic_ins2=list_for_atomic_ins;
                                        struct tm a= reverse_timestamp_form(tmp_atomic_ins->timestamp);
                                        
                                        while(tmp_atomic_ins2!=NULL){
                                            struct tm b= reverse_timestamp_form(tmp_atomic_ins2->timestamp);
                                            if(tmp_atomic_ins2->client==tmp_atomic_ins->client&&tmp_atomic_ins2->type!=tmp_atomic_ins->type&&tmp_atomic_ins2->timestamp<tmp_atomic_ins->timestamp&&a.tm_mday==b.tm_mday&&a.tm_mon==b.tm_mon&&a.tm_year==b.tm_year){
                                                tmp[5]=tmp_atomic_ins2->timestamp;
                                                //printf("I AM %d SENT ACK1 for client %d time_start <%d:%d:%d %d/%d/%d> time_end <%d:%d:%d %d/%d/%d>\n",rank,tmp[7],a.tm_hour, a.tm_min, a.tm_sec, a.tm_mday, a.tm_mon, a.tm_year,b.tm_hour, b.tm_min, b.tm_sec, b.tm_mday, b.tm_mon, b.tm_year);

                                            }
                                            //printf("I AM %d SENT ACK1 for client %d time_start <%d:%d:%d %d/%d/%d> time_end <%d:%d:%d %d/%d/%d>\n",rank,tmp[7],a.tm_hour, a.tm_min, a.tm_sec, a.tm_mday, a.tm_mon, a.tm_year,b.tm_hour, b.tm_min, b.tm_sec, b.tm_mday, b.tm_mon, b.tm_year);
                                            
                                            
                                            tmp_atomic_ins2=tmp_atomic_ins2->next;
                                        }
                                        
                                        MPI_Send(&tmp,1,TIMESTAMP,find_next_overtime(rank,c[2],num_of_server),OVERTIME_ACK,MPI_COMM_WORLD);
                                        
                                    }
                                    tmp_atomic_ins=tmp_atomic_ins->next;
                                }
                                //printf("I AM %d AND I SENT THA TA SPASW OLA %d\n",rank,tha_ta_spasw_ola);
                        }else if(rank==num_of_server*num_of_server-num_of_server+1 && tha_ta_spasw_ola==1){
                            tha_ta_spasw_ola=0;
                        }
                    }
                }



            }else if(status.MPI_TAG==OVERTIME_ACK){
                long int tmp[8];
                tmp[0]=c[0];
                tmp[1]=c[1];
                tmp[2]=rank;
                tmp[3]=c[3];
                tmp[4]=c[4];
                tmp[5]=c[5];
                tmp[6]=c[6];
                tmp[7]=c[7];
                
                
                if(c[4]!=rank){
                    //printf("I AM %d SENT ACK TORUS NEXT %d\n",rank,find_next_overtime(rank,c[2],num_of_server));
                    MPI_Send(&tmp,1,TIMESTAMP,find_next_overtime(rank,c[2],num_of_server),OVERTIME_ACK,MPI_COMM_WORLD);
                }else{
                    //printf("from %d\n",c[2]);
                    //printf("PLZ KILL ME %d from %d id %d\n",c[7],c[6]-c[5],rank);
                    counter_for_overtime_acks++;
                    //printf("total acks %d total %d\n",counter_for_overtime_acks,c[3]);
                    struct OverTimeList* temporary=(struct OverTimeList*) malloc(sizeof(struct OverTimeList));
                    temporary->id=c[7];
                    temporary->start_time=c[5];
                    temporary->end_time=c[6];
                    temporary->next=saved_overtimes;
                    saved_overtimes=temporary;
                }
                struct DList* fgfg=list_for_atomic_ins;
                int fgfd=0;
                while(fgfg!=NULL){
                    fgfd++;
                    fgfg=fgfg->next;
                }
                if(counter_for_overtime_acks+fgfd/2==(c[3]/2)){
                    overtime_flag=0;
                    struct DList *tmp_atomic_ins=list_for_atomic_ins;
                    while(tmp_atomic_ins!=NULL){
                        if(tmp_atomic_ins->type!=0){
                            // tmp[0]=c[0];
                            // tmp[1]=c[1];
                            // tmp[2]=c[2];
                            // tmp[3]=c[3];
                            // tmp[4]=c[4];
                            // tmp[5]=-1;   //in case of printing -1 something is terrible wrong with my life and all started at 2017 when i passed at CSD
                            // tmp[6]=tmp_atomic_ins->timestamp;
                            // tmp[7]=tmp_atomic_ins->client;

                            struct DList *tmp_atomic_ins2=tmp_atomic_ins;
                            while(tmp_atomic_ins2!=NULL){
                                if(tmp_atomic_ins2->client==tmp_atomic_ins->client&&tmp_atomic_ins2->type!=tmp_atomic_ins->type&&tmp_atomic_ins2->timestamp<tmp_atomic_ins->timestamp){
                                    
                                    struct OverTimeList* temporary=(struct OverTimeList*) malloc(sizeof(struct OverTimeList));
                                    temporary->id=tmp_atomic_ins->server;
                                    temporary->start_time=tmp_atomic_ins2->timestamp;;
                                    temporary->end_time=tmp_atomic_ins->timestamp;
                                    temporary->next=saved_overtimes;
                                    saved_overtimes=temporary;

                                }
                                tmp_atomic_ins2=tmp_atomic_ins2->next;
                            }

                            //MPI_Send(&tmp,1,TIMESTAMP,find_next_overtime(rank,c[2],num_of_server),OVERTIME_ACK,MPI_COMM_WORLD);
                        }
                        tmp_atomic_ins=tmp_atomic_ins->next;
                    }

                    // printf("I GOT ALL ACKS\n");
                    // printf("counter is %d c[3] is %d\n",counter_for_overtime_acks,c[3]);
                    counter_for_overtime_acks=0;
                    
                    struct OverTimeList* temporary=saved_overtimes;
                    struct List_count *print_for_overtime=NULL;
                    while(temporary!=NULL){
                        // if(temporary->start_time>temporary->end_time){
                        //     printf("MLKIAAAAAAAAAAAAAAAAAAAAAAA\n");
                        // }
                        if(temporary->start_time>c[0]&& temporary->end_time<c[1]&& temporary->end_time-temporary->start_time>28800){
                            struct List_count *gamw_tin_zoi_m_gia_overtime=print_for_overtime;
                            int flag=0;
                            while(gamw_tin_zoi_m_gia_overtime!=NULL){
                                if(gamw_tin_zoi_m_gia_overtime->id==temporary->id){
                                    flag=1;
                                    gamw_tin_zoi_m_gia_overtime->count=gamw_tin_zoi_m_gia_overtime->count+1;
                                }
                                gamw_tin_zoi_m_gia_overtime=gamw_tin_zoi_m_gia_overtime->next;
                            }
                            if(flag==0){
                                struct List_count *pfffffffff= (struct List_count*) malloc(sizeof(struct List_count));
                                pfffffffff->id=temporary->id;
                                pfffffffff->count=1;
                                pfffffffff->next=print_for_overtime;
                                print_for_overtime=pfffffffff;

                            }

                            

                            //printf("id %d has worked more than 8 hours start %d end %d\n",temporary->id,temporary->start_time,temporary->end_time);
                        }
                        temporary=temporary->next;
                    }
                    struct List_count *last_one=print_for_overtime;
                    while(last_one!=NULL){
                        printf("SERVER <%d> COUNTED <%d> OVERTIMES\n",last_one->id,last_one->count);
                        last_one=last_one->next;
                        
                    }

                    MPI_Send(&tmp,1,TIMESTAMP,0,OVERTIME_ACK,MPI_COMM_WORLD);
                }
                fgfd=0;
            }else if(status.MPI_TAG==PRINT){
                struct List *tmp_childrens=childrens;
                int num_printf_acks=0;

                // if(childrens==NULL){
                //     printf("got it %d\n",rank);
                    
                // }
                

                if(parent!=rank){
                    const int msgsize = 10;

                    struct DList*tmp_atomic_ins=list_for_atomic_ins;
                    long int total_records=0;
                    long int total_hours=0;
                    while(tmp_atomic_ins!=NULL){
                        if(tmp_atomic_ins->type!=0){
                            

                            struct DList *tmp_atomic_ins2=list_for_atomic_ins;
                            struct tm a= reverse_timestamp_form(tmp_atomic_ins->timestamp);
                            
                            while(tmp_atomic_ins2!=NULL){
                                struct tm b= reverse_timestamp_form(tmp_atomic_ins2->timestamp);
                                if(tmp_atomic_ins2->client==tmp_atomic_ins->client&&tmp_atomic_ins2->type!=tmp_atomic_ins->type&&tmp_atomic_ins2->timestamp<tmp_atomic_ins->timestamp&&a.tm_mday==b.tm_mday&&a.tm_mon==b.tm_mon&&a.tm_year==b.tm_year){
                                    // tmp[5]=tmp_atomic_ins2->timestamp;
                                    // printf("I AM %d SENT ACK1 for client %d time_start <%d:%d:%d %d/%d/%d> time_end <%d:%d:%d %d/%d/%d>\n",rank,tmp[7],a.tm_hour, a.tm_min, a.tm_sec, a.tm_mday, a.tm_mon, a.tm_year,b.tm_hour, b.tm_min, b.tm_sec, b.tm_mday, b.tm_mon, b.tm_year);
                                    struct tm b= reverse_timestamp_form(tmp_atomic_ins2->timestamp);
                                    total_hours+=a.tm_hour-b.tm_hour;
                                }
                                //printf("I AM %d SENT ACK1 for client %d time_start <%d:%d:%d %d/%d/%d> time_end <%d:%d:%d %d/%d/%d>\n",rank,tmp[7],a.tm_hour, a.tm_min, a.tm_sec, a.tm_mday, a.tm_mon, a.tm_year,b.tm_hour, b.tm_min, b.tm_sec, b.tm_mday, b.tm_mon, b.tm_year);
                                  
                                tmp_atomic_ins2=tmp_atomic_ins2->next;
                            }
                            
                            
                        }
                        tmp_atomic_ins=tmp_atomic_ins->next;
                        total_records++;
                    }
                    char test[msgsize+1];            
                    sprintf(test, "SERVER <%d> HAS <%d> RECORDS WITH <%d> HOURS", rank,total_records,total_hours);
                    //MPI_File_write(handle,test,strlen(test), MPI_CHAR,&status);
                    //printf("i am %d writing to file\n",rank);
                    
                    //printf("i am %d handler\n",rank);
                    //MPI_File_open(MPI_COMM_WORLD, "helloworld2.txt", MPI_MODE_CREATE | MPI_MODE_WRONLY ,MPI_INFO_NULL, &handle);
                    //printf("i am %d opened file\n",rank);
                    //printf("i am %d seek file\n",rank);
                    MPI_File_write(handle,test,strlen(test), MPI_CHAR,&status);
                    //MPI_File_write(handle,test,strlen(test), MPI_CHAR,&status);
                    //printf("i am %d writed to file\n",rank);
                    
                    //printf("i am %d done\n",rank);
                    while(tmp_childrens!=NULL){
                        MPI_Send(&c,1,TIMESTAMP,tmp_childrens->data,PRINT,MPI_COMM_WORLD);
                        tmp_childrens=tmp_childrens->next;
                    }
                    MPI_Send(&c,1,TIMESTAMP,parent,PRINT_ACK,MPI_COMM_WORLD);
                    //printf("got it sent print ack %d\n",rank);
                    
                }else{
                    while(tmp_childrens!=NULL){
                        MPI_Send(&c,1,TIMESTAMP,tmp_childrens->data,PRINT,MPI_COMM_WORLD);
                        tmp_childrens=tmp_childrens->next;
                    }
                    while(num_printf_acks!=num_of_server*num_of_server-1){
                        MPI_Recv(&c, 1, TIMESTAMP, MPI_ANY_SOURCE, PRINT_ACK, MPI_COMM_WORLD, &status);
                        num_printf_acks++;
                    }
                    //printf("got all print ack leader\n");

                    const int msgsize = 10;

                    struct DList*tmp_atomic_ins=list_for_atomic_ins;
                    long int total_records=0;
                    long int total_hours=0;
                    while(tmp_atomic_ins!=NULL){
                        if(tmp_atomic_ins->type!=0){
                            

                            struct DList *tmp_atomic_ins2=list_for_atomic_ins;
                            struct tm a= reverse_timestamp_form(tmp_atomic_ins->timestamp);
                            
                            while(tmp_atomic_ins2!=NULL){
                                struct tm b= reverse_timestamp_form(tmp_atomic_ins2->timestamp);
                                if(tmp_atomic_ins2->client==tmp_atomic_ins->client&&tmp_atomic_ins2->type!=tmp_atomic_ins->type&&tmp_atomic_ins2->timestamp<tmp_atomic_ins->timestamp&&a.tm_mday==b.tm_mday&&a.tm_mon==b.tm_mon&&a.tm_year==b.tm_year){
                                    // tmp[5]=tmp_atomic_ins2->timestamp;
                                    // printf("I AM %d SENT ACK1 for client %d time_start <%d:%d:%d %d/%d/%d> time_end <%d:%d:%d %d/%d/%d>\n",rank,tmp[7],a.tm_hour, a.tm_min, a.tm_sec, a.tm_mday, a.tm_mon, a.tm_year,b.tm_hour, b.tm_min, b.tm_sec, b.tm_mday, b.tm_mon, b.tm_year);
                                    struct tm b= reverse_timestamp_form(tmp_atomic_ins2->timestamp);
                                    total_hours+=a.tm_hour-b.tm_hour;
                                }
                                //printf("I AM %d SENT ACK1 for client %d time_start <%d:%d:%d %d/%d/%d> time_end <%d:%d:%d %d/%d/%d>\n",rank,tmp[7],a.tm_hour, a.tm_min, a.tm_sec, a.tm_mday, a.tm_mon, a.tm_year,b.tm_hour, b.tm_min, b.tm_sec, b.tm_mday, b.tm_mon, b.tm_year);
                                  
                                tmp_atomic_ins2=tmp_atomic_ins2->next;
                            }
                            
                            
                        }
                        tmp_atomic_ins=tmp_atomic_ins->next;
                        total_records++;
                    }
                    char test[msgsize+1];            
                    sprintf(test, "SERVER <%d> HAS <%d> RECORDS WITH <%d> HOURS", rank,total_records,total_hours);
                    //MPI_File_write(handle,test,strlen(test), MPI_CHAR,&status);
                    //printf("i am %d writing to file\n",rank);
                    
                    //printf("i am %d handler\n",rank);
                    //MPI_File_open(MPI_COMM_WORLD, "helloworld2.txt", MPI_MODE_CREATE | MPI_MODE_WRONLY ,MPI_INFO_NULL, &handle);
                    //printf("i am %d opened file\n",rank);
                    //printf("i am %d seek file\n",rank);
                    MPI_File_write(handle,test,strlen(test), MPI_CHAR,&status);
                    //MPI_File_write(handle,test,strlen(test), MPI_CHAR,&status);

                    MPI_Send(&c,1,TIMESTAMP,0,PRINT_ACK,MPI_COMM_WORLD);
                }



            }else if(status.MPI_TAG==PRINT_ACK){

                
                MPI_Send(&c,1,TIMESTAMP,parent,PRINT_ACK,MPI_COMM_WORLD);
                


            }else if(status.MPI_TAG==TOTAL_TERMINATE){

                
                break;
                


            }
            

        }



    }else{              //clients



        //connection
       while(1){
            int c[2];
            MPI_Recv(&c, 1, CUSTOM_ARRAY_for_connect, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
            if(status.MPI_TAG==FROM_SERVER){
                struct List*a=(struct List*) malloc(sizeof(struct List));
                if(c[0]==rank)
                    a->data=c[1];
                else 
                    a->data=c[0];
                
                a->next=neighbors;
                neighbors=a;
                if(c[0]==rank){
                    MPI_Send(&c,1,CUSTOM_ARRAY_for_connect,c[1],FROM_SERVER,MPI_COMM_WORLD);
                    MPI_Recv(&a, 1, MPI_CHAR, MPI_ANY_SOURCE, ACK, MPI_COMM_WORLD, &status);
                    int a='a';
                    //printf("rank: %d connected: %d\n",rank,c[1]);
                    struct List * lala=neighbors;
                    //printf("rank: %d\n",rank);
                    // while(lala!=NULL){
                    //     printf("%d ",lala->data);
                    //     lala=lala->next;
                    // }
                    //printf("\n");
                    
                    MPI_Send(&a,1,MPI_CHAR,0,ACK,MPI_COMM_WORLD);
                    
                }else{
                    char a='a';
                    //printf("rank: %d connected: %d\n",rank,c[0]);
                    struct List * lala=neighbors;
                    //printf("rank: %d\n",rank);
                    // while(lala!=NULL){
                    //     printf("%d ",lala->data);
                    //     lala=lala->next;
                    // }
                    //printf("\n");
                    MPI_Send(&a,1,MPI_CHAR,c[0],ACK,MPI_COMM_WORLD);
                }
            } else{
                break;
            }

        }

        //leader election clients
        while(1){
            int c[4];
            if(election_flag)
                break;
            MPI_Recv(&c, 1, CUSTOM_ARRAY, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
            if(status.MPI_TAG==START_LEADER_ELECTION_CLIENTS){
                    int thrash=0;
                    MPI_Send(&thrash,1,MPI_INT,0,ACK,MPI_COMM_WORLD);
                    MPI_Recv(&c, 1, CUSTOM_ARRAY, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

                    struct List * lala=neighbors;
                    int i=0;
                    //printf("rank: %d\n",rank);
                    while(lala!=NULL){
                        //printf("%d ",lala->data);
                        
                        lala=lala->next;
                        i++;
                    }
                    lala=neighbors;
                    if(i==1){
                        int temp[4];
                        temp[0]=rank;
                        temp[1]=rank;
                        temp[2]=rank;
                        temp[3]=rank;
                        struct List*hayate=(struct List*) malloc(sizeof(struct List));
                        hayate->data=lala->data;
                        hayate->next=send;
                        send=hayate;
                        parent=lala->data;
                        //printf("my rank %d to send rank %d\n",rank,lala->data);
                        MPI_Send(&temp,1,CUSTOM_ARRAY,lala->data,ELECT,MPI_COMM_WORLD);
                    }

            }else if(status.MPI_TAG==ELECT){
                
                int num_neighbors=0,num_received=0,num_send=0;


                struct List*hayate=(struct List*) malloc(sizeof(struct List));
                hayate->data=c[0];
                hayate->next=received;
                received=hayate;

                struct List * lala=neighbors;
                struct List*tmp_received=received;
                struct List*tmp_send=send;
                while(lala!=NULL){
                        //printf("%d ",lala->data);
                        
                        lala=lala->next;
                        num_neighbors++;
                }
                while(tmp_received!=NULL){
                        tmp_received=tmp_received->next;
                        num_received++;
                }

                while(tmp_send!=NULL){
                        tmp_send=tmp_send->next;
                        num_send++;
                }



                lala=neighbors;
                tmp_received=received;
                tmp_send=send;
                


                if(num_received==num_neighbors-1){
                    struct List*haha=(struct List*) malloc(sizeof(struct List));
                    int not_rec_neigh=-1;

                    while(lala!=NULL&&not_rec_neigh==-1){
                       tmp_received=received;
                       not_rec_neigh=lala->data;
                       while(tmp_received!=NULL){
                            if(lala->data==tmp_received->data){
                                not_rec_neigh=-1;
                            }
                            tmp_received=tmp_received->next;
                       }
                       lala=lala->next; 
                    }

                    haha->data=not_rec_neigh;
                    haha->next=send;
                    send=haha;
                    int temp_array[4];
                    temp_array[0]=rank;
                    temp_array[1]=rank;
                    temp_array[2]=rank;
                    temp_array[3]=rank;
                    parent=not_rec_neigh;
                    MPI_Send(&temp_array,1,CUSTOM_ARRAY,not_rec_neigh,ELECT,MPI_COMM_WORLD);
                }else if(num_received==num_neighbors&& num_send==0){
                    //printf("I AM CLIENT LEADER1 %d\n",rank);
                    int l[4];
                    l[0]=rank;
                    l[1]=rank;
                    l[2]=rank;
                    l[3]=rank;
                    int ji;
                    client_leader=rank;
                    parent=rank;
                    for(ji=num_of_server*num_of_server+1;ji<world_size;ji++){
                        if(ji!=rank)
                            MPI_Send(&l,1,CUSTOM_ARRAY,ji,I_AM_LEADER_CLIENT,MPI_COMM_WORLD);
                    }
                    ji=rank;
                    int counter=0;
                    while(counter!=world_size-num_of_server*num_of_server-2){
                        MPI_Recv(&c, 1, CUSTOM_ARRAY, MPI_ANY_SOURCE, ACK, MPI_COMM_WORLD, &status);
                        counter++;
                    }
                    MPI_Send(&ji,1,MPI_INT,0,LEADER_ELECTION_DONE_CLIENTS,MPI_COMM_WORLD);
                    election_flag=1;
                }

                lala=neighbors;
                tmp_received=received;
                tmp_send=send;
                while(tmp_received!=NULL){
                       tmp_send=send;
                       while(tmp_send!=NULL){
                            if(tmp_send->data==tmp_received->data){
                                if(rank>tmp_received->data){
                                    //printf("I AM CLIENT LEADER2 %d\n",rank);
                                    client_leader=rank;
                                    int l[4];
                                    l[0]=rank;
                                    l[1]=rank;
                                    l[2]=rank;
                                    l[3]=rank;
                                    int ji;
                                    parent=rank;
                                    for(ji=num_of_server*num_of_server+1;ji<world_size;ji++)
                                        if(ji!=rank)
                                            MPI_Send(&l,1,CUSTOM_ARRAY,ji,I_AM_LEADER_CLIENT,MPI_COMM_WORLD);
                                    
                                    ji=rank;
                                    int counter=0;
                                    while(counter!=world_size-num_of_server*num_of_server-2){
                                            MPI_Recv(&c, 1, CUSTOM_ARRAY, MPI_ANY_SOURCE, ACK, MPI_COMM_WORLD, &status);
                                            counter++;
                                            //printf("%d %d\n",counter,world_size-num_of_server*num_of_server);
                                    }
                                    MPI_Send(&ji,1,MPI_INT,0,LEADER_ELECTION_DONE_CLIENTS,MPI_COMM_WORLD);
                                    election_flag=1;
                                    
                                }else parent=tmp_received->data;
                            }
                            tmp_send=tmp_send->next;
                       }
                       tmp_received=tmp_received->next; 
                    }

            
            }else if(status.MPI_TAG==I_AM_LEADER_CLIENT){
                //printf("RANK: %d LEADER: %d\n",rank,c[0]);
                client_leader=c[0];
                MPI_Send(&c,1,CUSTOM_ARRAY,client_leader,ACK,MPI_COMM_WORLD);
                break;
            }

        }

        //c0 leader client
        //c1 to server
        //c2 timestamp
        //c3 to client
        //c4 pos
        //c5 in or out
        
        while(1){
            long int array[8];
            MPI_Recv(&array, 1, TIMESTAMP, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
            if(status.MPI_TAG==REGISTER_IN){
                
                if(array[3]==rank && rank==parent){
                    //register_number++;
                    //printf("rank %d got register from1 %d\n",rank,array[1]);
                    array[0]=rank;
                    MPI_Send(&array,1,TIMESTAMP,num_of_server*num_of_server,FORWARD,MPI_COMM_WORLD);
                    //printf("W8 for ack %d\n",rank);
                    register_number++;

                    MPI_Recv(&array, 1, TIMESTAMP, MPI_ANY_SOURCE, ACK, MPI_COMM_WORLD, &status);
                    
                    printf("CLIENT <%d> REGISTERED <IN><%ld>\n",rank,array[2]);
                    
                    MPI_Send(&array,1,TIMESTAMP,0,ACK,MPI_COMM_WORLD); 
                }else{
                    if(array[3]==rank){
                        MPI_Send(&array,1,TIMESTAMP,parent,REGISTER_IN,MPI_COMM_WORLD);
                        //printf("W8 for ack %d\n",rank);
                        register_number++;
                        MPI_Recv(&array, 1, TIMESTAMP, MPI_ANY_SOURCE, ACK, MPI_COMM_WORLD, &status);
                        printf("CLIENT <%d> REGISTERED <IN><%ld>\n",rank,array[2]);
                        //printf("sent to cord %d\n",rank);
                        MPI_Send(&array,1,TIMESTAMP,0,ACK,MPI_COMM_WORLD);   
                    }
                    else if(rank!=parent ){
                        MPI_Send(&array,1,TIMESTAMP,parent,REGISTER_IN,MPI_COMM_WORLD);
                    }else{
                        //register_number++;
                        //printf("rank %d got register from2 %d\n",rank,array[1]);
                        array[0]=rank;
                        MPI_Send(&array,1,TIMESTAMP,num_of_server*num_of_server,FORWARD,MPI_COMM_WORLD);
                        
                    }
                }
            }else if(status.MPI_TAG==REGISTER_OUT){
                
                if(array[3]==rank && rank==parent){
                    //register_number++;
                    //printf("rank %d got register from1 %d\n",rank,array[1]);
                    array[0]=rank;
                    MPI_Send(&array,1,TIMESTAMP,num_of_server*num_of_server,FORWARD,MPI_COMM_WORLD);
                    //printf("W8 for ack %d\n",rank);
                    register_number++;
                    MPI_Recv(&array, 1, TIMESTAMP, MPI_ANY_SOURCE, ACK, MPI_COMM_WORLD, &status);
                    
                    printf("CLIENT <%d> REGISTERED <OUT><%ld>\n",rank,array[2]);
                    //printf("sent to cord %d\n",rank);
                    MPI_Send(&array,1,TIMESTAMP,0,ACK,MPI_COMM_WORLD); 
                }else{
                    if(array[3]==rank){
                        MPI_Send(&array,1,TIMESTAMP,parent,REGISTER_OUT,MPI_COMM_WORLD);
                        //printf("W8 for ack %d\n",rank);
                        register_number++;
                        MPI_Recv(&array, 1, TIMESTAMP, MPI_ANY_SOURCE, ACK, MPI_COMM_WORLD, &status);
                        //printf("sent to cord %d\n",rank);
                        printf("CLIENT <%d> REGISTERED <OUT><%ld>\n",rank,array[2]);
                        MPI_Send(&array,1,TIMESTAMP,0,ACK,MPI_COMM_WORLD);   
                    }
                    else if(rank!=parent ){
                        MPI_Send(&array,1,TIMESTAMP,parent,REGISTER_OUT,MPI_COMM_WORLD);
                    }else{
                        //register_number++;
                        //printf("rank %d got register from2 %d\n",rank,array[1]);
                        array[0]=rank;
                        MPI_Send(&array,1,TIMESTAMP,num_of_server*num_of_server,FORWARD,MPI_COMM_WORLD);
                        
                    }
                }

            }else if(status.MPI_TAG==PRINT){
                if(rank==parent){
                    
                    struct List *tmp_neighbors=neighbors;
                    array[0]=rank;
                    while(tmp_neighbors!=NULL){
                        MPI_Send(&array,1,TIMESTAMP,tmp_neighbors->data,PRINT,MPI_COMM_WORLD);
                        tmp_neighbors=tmp_neighbors->next;
                    }
                    
                }else{
                    const int msgsize = 10;

                    //struct DList*tmp_atomic_ins=list_for_atomic_ins;
                    
                    char test[msgsize+1];            
                    sprintf(test, "CLIENT <%d> PROCESSED <%d> REQUESTS", rank,register_number);
                    //MPI_File_write(handle,test,strlen(test), MPI_CHAR,&status);
                    //printf("i am %d writing to file\n",rank);
                    
                    //printf("i am %d handler\n",rank);
                    //MPI_File_open(MPI_COMM_WORLD, "helloworld2.txt", MPI_MODE_CREATE | MPI_MODE_WRONLY ,MPI_INFO_NULL, &handle);
                    //printf("i am %d opened file\n",rank);
                    //printf("i am %d seek file\n",rank);
                    MPI_File_write(handle,test,strlen(test), MPI_CHAR,&status);
                    //MPI_File_write(handle,test,strlen(test), MPI_CHAR,&status);
                    //printf("i am %d writed to file\n",rank);
                    
                    //printf("i am %d done\n",rank);
                    struct List *tmp_neighbors=neighbors;
                    long int x[8];
                    x[0]=rank;
                    while(tmp_neighbors!=NULL){
                        if(tmp_neighbors->data!=array[0])
                            MPI_Send(&x,1,TIMESTAMP,tmp_neighbors->data,PRINT,MPI_COMM_WORLD);
                        tmp_neighbors=tmp_neighbors->next;
                    }
                    MPI_Send(&array,1,TIMESTAMP,parent,PRINT_ACK,MPI_COMM_WORLD);

                }

            }else if(status.MPI_TAG==PRINT_ACK){
                int print_ack_counter=1;
                if(rank!=parent){
                    MPI_Send(&array,1,TIMESTAMP,parent,PRINT_ACK,MPI_COMM_WORLD);
                }else{

                    while(print_ack_counter!=world_size-num_of_server*num_of_server-2){
                        MPI_Recv(&array, 1, TIMESTAMP, MPI_ANY_SOURCE , PRINT_ACK , MPI_COMM_WORLD, &status);
                        print_ack_counter++;
                        //printf("req rec %d clients %d\n",print_ack_counter,world_size-num_of_server*num_of_server-1);
                    }
                    //printf("i got all acks client leaader %d\n",print_ack_counter);
                    const int msgsize = 10;

                    //struct DList*tmp_atomic_ins=list_for_atomic_ins;
                    
                    char test[msgsize+1];            
                    sprintf(test, "CLIENT <%d> PROCESSED <%d> REQUESTS", rank,register_number);
                    //MPI_File_write(handle,test,strlen(test), MPI_CHAR,&status);
                    //printf("i am %d writing to file\n",rank);
                    
                    //printf("i am %d handler\n",rank);
                    //MPI_File_open(MPI_COMM_WORLD, "helloworld2.txt", MPI_MODE_CREATE | MPI_MODE_WRONLY ,MPI_INFO_NULL, &handle);
                    //printf("i am %d opened file\n",rank);
                    //printf("i am %d seek file\n",rank);
                    MPI_File_write(handle,test,strlen(test), MPI_CHAR,&status);
                    //MPI_File_write(handle,test,strlen(test), MPI_CHAR,&status);
                    //printf("i am %d writed to file\n",rank);
                    
                    //printf("i am %d done\n",rank);

                    MPI_Send(&array,1,TIMESTAMP,0,PRINT_ACK,MPI_COMM_WORLD);
                    
                }


            }else if(status.MPI_TAG==TOTAL_TERMINATE){
                break;
            }
        }


    }
    //printf("I AM READY TO DIE %d\n",rank);
    MPI_File_close(&handle);
    MPI_Finalize();
    
}