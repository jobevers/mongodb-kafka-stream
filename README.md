# Mongo Kafka Connector


## Local Development and Testing

### Run mongo

I had a hard time getting the networking right with docker
so I found it best to run mongo as a local process:
```
mongod --replSet rs0 --dbpath $PWD/rs0-1 --smallfiles
```

### Run kafka

Docker works well here:

```
docker-compose -f docker-compose-single-broker.yml up
```

### Run the app

```
go run main.go
```

### Create messages
I have a notebook that can be run that

 1. Writes new documents to mongo
 2. Reads from kafka

```
jupyter notebook "Stress Test.ipynb"
```

It also has some basic stats to look at timestamps
and make sure that no messages are lost.

