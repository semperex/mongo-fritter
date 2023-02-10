# MONGO FRITTER [pre-release]
The Mongo Fritter's purpose is to get you running fast and intuitively with Java and MongoDB.

It is a wrapper around MongoDB's Java client to simplify access to data with almost-POJO data models.

## Key Features
+ Easy integration of models
+ Pre-built DAO functionality (as abstract bases)
+ Optional auto-assignment of collection names
+ Streaming retrieval supports bigger data
+ Login credentials in (gasp) environment variables
+ Works with Atlas

## Users & Lead Link Media Sponsorship

The project was originally built for applications in financial trading where it powered
solutions on time series data and pricing history. 

Mongo Fritter is now open sourced under MIT license with the sponsorship of user
[Lead Link Media](https://leadlinkmedia.com/), 
an auto-glass marketing company.  Mongo Fritter is behind solutions that power 
Lead Link Media's near real-time call information on the web, internal analytics
and more.

## Quick Start

Create the model class:
```java
package com.company.models;

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

Create the DAO class:
```java
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
