# Pet Clinic Index Job

Source code for [Enabling Full-text Search with Change Data Capture in a Legacy Application](https://jet-start.sh/blog/2020/10/06/enabling-full-text-search)
blog post.

## How to

Follow these instructions to run the Spring Petclinic application and
the PetClinicIndexJob:

Start MySQL database:

```bash
docker run --name petclinic-mysql -it \
  -e MYSQL_DATABASE=petclinic \
  -e MYSQL_USER=petclinic \
  -e MYSQL_PASSWORD=petclinic \
  -e MYSQL_ROOT_PASSWORD=mysql \
  -p 3306:3306 mysql
```

Start MySQL client (enter `mysql` password for root user):

```bash
docker run -it --rm --link petclinic-mysql:petclinic-mysql mysql mysql -hpetclinic-mysql -uroot -p
```

Grant privileges for change data capture:

```sql
ALTER USER petclinic IDENTIFIED WITH mysql_native_password BY 'petclinic';
GRANT RELOAD ON *.* TO 'petclinic';
GRANT REPLICATION CLIENT ON *.* TO 'petclinic';
GRANT REPLICATION SLAVE ON *.* TO 'petclinic';
```

Start Elasticsearch

```bash
docker run --name petclinic-elastic \
  -e discovery.type=single-node \
  -e cluster.routing.allocation.disk.threshold_enabled=false \
  -p9200:9200 elasticsearch:7.9.2
```

Create an Elasticsearch index mapping:

```bash
curl -XPUT -H "Content-type: application/json" -d '
{
  "mappings": {
    "properties": {
      "first_name": {
        "type": "text",
        "copy_to": "search"
      },
      "last_name": {
        "type": "text",
        "copy_to": "search"
      },
      "pets.name": {
        "type": "text",
        "copy_to": "search"
      },
      "pets.visits.keywords": {
        "type": "text",
        "copy_to": "search"
      },
      "search": {
        "type": "text"
      }
    }
  }
}' http://localhost:9200/petclinic-index
```

Clone the Spring Petclinic repository and checkout `elasticsearch` branch:

```bash
git clone https://github.com/hazelcast-demos/spring-petclinic.git
git checkout elasticsearch
```

Run the Petclinic application:

```bash
cd spring-petclinic
./mvnw spring-boot:run -Dspring-boot.run.profiles=mysql
```

Clone the PetClinicIndexJob repository:

```bash
git clone https://github.com/hazelcast-demos/pet-clinic-index-job.git
```

Build the jar:

```bash
cd pet-clinic-indexing-job
mvn package
```

(Option 1)
Run the job (embedded, without submitting to a cluster)

```bash
mvn exec:java -Pstandalone
```

(Option 2)
Submit the job to a cluster (you need to have modules elasticsearch-7, 
cdc-debezium and cdc-mysql enabled - copy the jars from opt to lib 
folder):

```bash
bin/jet submit \
path/to/pet-clinic-index-job/target/pet-clinic-index-job-1.0-SNAPSHOT-jar-with-dependencies.jar\
   --database-address localhost \
   --database-port 3306 \
   --database-user petclinic \
   --database-password petclinic \
   --elastic-host localhost:9200 \
   --elastic-index petclinic-index
```
