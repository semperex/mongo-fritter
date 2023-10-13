# MONGO FRITTER [pre-release]
The Mongo Fritter's purpose is to get you running fast and intuitively with Java and MongoDB.

It is a wrapper around MongoDB's Java client to simplify access to data with almost-POJO data models.

## Key Features
+ Nearly POJO models
+ Pre-built, extensible DAO functionality (as abstract bases)
+ Optional auto-assignment of collection names
+ Streaming retrieval supports bigger data
+ Login credentials in (gasp) environment variables
+ Works with Atlas

## Users & Lead Link Media Sponsorship

The project was originally built for applications in financial trading where it powered solutions 
on time series data, price history and real-time order placement. 

Mongo Fritter is now open sourced under MIT license with the sponsorship of user
[Lead Link Media](https://leadlinkmedia.com/), 
an auto-glass marketing company.  Mongo Fritter is behind solutions that power 
Lead Link Media's near real-time call information on the web, internal analytics
and more.

## Getting Started

Set environment variables:
```shell
MONGODB_USERNAME=my_username
MONGODB_PASSWORD=my_password
```

Create the model class:
```java
package com.company.models;

import com.semperex.mongo_fritter.model.AbstractModel;

class Pet extends AbstractModel<Long> {

    private String name;

    public Pet(Long id, String name) {
        super(id);
        this.name = name;
    }

    public String getName() {
        return this.name;
    }
    
}
```

Create the DAO class with a connection to local MongoDB server:
```java
import com.semperex.mongo_fritter.dao.DAOBase;

class PetDAO extends DAOBase<Pet, Long> {
    public PetDAO() {
        super(
            MongoDBPOJOConnectionCreatorBuilder.builder()
                .withPojoPackageName("com.company.models")
                .withDatabaseName("my_database")
                .withLocalServerAddress()
                .build());
    }
}
```

Or create the DAO class with a connection to local or MongoDB Atlas service depending on environment and also make the database name configurable.  (You may want to build your own reusable base class with this logic.):
```java

public enum Environment { DEVELOPMENT, PRODUCTION };

...

import com.semperex.mongo_fritter.dao.DAOBase;

// from Maven package: https://mvnrepository.com/artifact/org.apache.commons/commons-lang3
import org.apache.commons.lang3.StringUtils;

class PetDAO extends DAOBase<Pet, Long> {

    // your code to get environment here
    private static final Environment environment = Enviroment.PRODUCTION;
  
    private static final String databaseFromEnv = System.getenv("MONGODB_DATABASE");

    public PetDAO() {
        super(
            switch ( environment ) {

                // Connect to local
                case DEVELOPMENT -> MongoDBPOJOConnectionCreatorBuilder.builder()
                        .withPojoPackageName("com.company.models")
                        .withDatabaseName( (StringUtils.isBlank(databaseFromEnv)) ?
                                             "my_database" : databaseFromEnv )
                        .withLocalServerAddress()
                        .build();

                // Connect to MongoDB Atlas
                case PRODUCTION -> MongoDBPOJOConnectionCreatorBuilder.builder()
                        .withPojoPackageName("com.company.models")
                        .withDatabaseName( (StringUtils.isBlank(databaseFromEnv)) ?
                                             "my_database" : databaseFromEnv )
                        .withClusterName("cluster-name.mongodb.net")
                        .build();
            }
        );
    }
    ...
}
```

Instantiate the DAO:
```java
PetDAO petDAO = new PetDAO();
```

Instantiate the model:
```java
Pet sparky = new Pet(1, "Sparky");
```

Store sparky:
```java
petDAO.createOrUpdate(sparky);
```

Find sparky:
```java
petDAO.findByField("name", "sparky", result -> System.out.println(result.getName()));
```

Optionally add an index for more speed:
```java
...
class PetDAO extends DAOBase<Pet, Long> {
...
    public PetDAO() {
        ...
        createIndex("name");  // creates an index IF it does not already exist
        ...
    }
    ...
}

## What's "Nearly" POJO?

Currently, models need to extend AbstractModel.  Otherwise it is POJO.  Will probably remove this requirement with some reflection magic in the future, and then we can say "all" POJO.
