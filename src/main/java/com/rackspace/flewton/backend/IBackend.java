package com.rackspace.flewton.backend;


import com.rackspace.flewton.AbstractRecord;

/** intended mainly for scripting languages that cannot easily inherit from a java class. */
public interface IBackend {
    public void write(AbstractRecord record);
}
