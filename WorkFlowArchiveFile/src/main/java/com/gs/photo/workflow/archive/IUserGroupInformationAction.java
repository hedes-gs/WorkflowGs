package com.gs.photo.workflow.archive;

import java.io.IOException;
import java.security.PrivilegedAction;

public interface IUserGroupInformationAction { public <T> T run(PrivilegedAction<T> action) throws IOException; }
