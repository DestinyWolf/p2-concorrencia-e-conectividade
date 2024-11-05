#include <stdlib.h>

int main() {
    system("docker image prune");
    system("docker container prune");
    system("docker pull mongodb/mongodb-community-server:latest");
    system("docker run --name mongodb -p 27018:27017 -d mongodb/mongodb-community-server:latest");

    return 0;
}
