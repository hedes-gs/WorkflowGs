package com.gs.photos.ws.repositories;

import java.io.IOException;
import java.util.List;

public interface IKeywordsRepository {

    List<String> getAllKeywords() throws IOException;

    List<String> getAllKeywordsLike(String keyword) throws IOException;

}
