package mongo_fritter.dao;

import mongo_fritter.model.Model;
import org.bson.codecs.Codec;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

public interface DAO<PrimaryModelClassT extends Model<IdT>, DAOT extends DAO, IdT> {

    Class<PrimaryModelClassT> getModelClass();

    Class<? extends DAOT> getGenericDAOClass();

    default Collection<? extends Codec> getCodecs() {
        return null;
    }

    PrimaryModelClassT create() throws DAOException;

    PrimaryModelClassT create(final PrimaryModelClassT value) throws DAOException;

    void update(
            final PrimaryModelClassT value) throws DAOException;

    void findAll(
            final Consumer<PrimaryModelClassT> consumer) throws DAOException;

    List<PrimaryModelClassT> findAll() throws DAOException;

    PrimaryModelClassT findById(IdT id) throws DAOException;

    void deleteById(IdT id) throws DAOException;

    PrimaryModelClassT createOrUpdate(PrimaryModelClassT value) throws DAOException;

    PrimaryModelClassT createOrIgnore(PrimaryModelClassT value) throws DAOException;

}
