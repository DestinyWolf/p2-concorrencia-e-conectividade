#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

//numero de threads a serem execultadas
#define NUM_THREADS 20
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
int start_thread = 1;

void *routine(void*args) {
    pthread_mutex_lock(&mutex);
    while (start_thread) 
    {
        pthread_cond_wait(&cond, &mutex);
    }
    pthread_mutex_unlock(&mutex);
    
    system("python -m clientSide.terminais");
}
void *routine2(void*args) {
    pthread_mutex_lock(&mutex);
    while (start_thread) 
    {
        pthread_cond_wait(&cond, &mutex);
    }
    pthread_mutex_unlock(&mutex);
    
    system("python -m clientSide.terminais2");
}
void *routine3(void*args) {
    pthread_mutex_lock(&mutex);
    while (start_thread) 
    {
        pthread_cond_wait(&cond, &mutex);
    }
    pthread_mutex_unlock(&mutex);
    
    system("python -m clientSide.terminais3");
}

int main(int argc, char const *argv[])
{
    pthread_t thread[NUM_THREADS*3];
    for (int i = 0; i < NUM_THREADS; i++)
    {
        pthread_create(&thread[i*3], NULL, routine, NULL);
        pthread_create(&thread[i*3+1], NULL, routine2, NULL);
        pthread_create(&thread[i*3+2], NULL, routine3, NULL);
    }
    printf("threads criadas\n");
    pthread_mutex_lock(&mutex);
    start_thread = 0;
    pthread_cond_broadcast(&cond);
    pthread_mutex_unlock(&mutex);
    printf("threads iniciadas\n");
    for (int i = 0; i < NUM_THREADS; i++)
    {
        pthread_join(thread[i], NULL);
    }
    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&cond);

    return 0;
}
