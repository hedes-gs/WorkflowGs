package com.gs.photos.repositories;

import java.io.IOException;
import java.util.List;

public interface IPersonsRepository {

    List<String> getAllPersons() throws IOException;

    List<String> getAllPersonsLike(String person) throws IOException;

}
