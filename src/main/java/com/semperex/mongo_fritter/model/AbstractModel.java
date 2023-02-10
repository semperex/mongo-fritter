package com.semperex.mongo_fritter.model;

public abstract class AbstractModel<IdT> implements Model<IdT> {

    private IdT id;

    public AbstractModel(IdT id) {
        this.id = id;
    }

    public AbstractModel() {
        this.id = null;
    }

    @Override
    public void setId(IdT id) {
        this.id = id;
    }

    @Override
    public IdT getId() {
        return id;
    }

    @Override
    public String toString() {
        return "AbstractModel{" +
                "id=" + id +
                '}';
    }

}
