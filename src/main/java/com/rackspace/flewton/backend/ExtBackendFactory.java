/*
 * Copyright (c) 2010 Rackspace
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

package com.rackspace.flewton.backend;

import com.rackspace.flewton.ConfigError;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.python.util.PythonInterpreter;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class ExtBackendFactory {
    private static PythonInterpreter pyInterpreter = null;
    
    private static String[] backendSearchPaths = new ArrayList<String>() {{
        if (System.getProperty("flewton.backend_path") != null)
            add(System.getProperty("flewton.backend_path"));
        add("/etc/flewton/backends");
    }}.toArray(new String[0]);

    /**
     * creates a backend that is implemented in Python.
     * @param modulePath location of the python module. Starts with "py/"
     * @param config configuration.
     */
    public static IBackend createPythonBackend(String modulePath, HierarchicalConfiguration config) {
        if (pyInterpreter == null)
            pyInterpreter = new PythonInterpreter();
        pyInterpreter.execfile(getExternalResource(modulePath));
        String pyClassName = modulePath.substring(modulePath.lastIndexOf("/") + 1);
        pyClassName = pyClassName.substring(0, pyClassName.indexOf("."));
        String instanceName = "instance_of_" + pyClassName;
        String configName = instanceName + "_config";
        pyInterpreter.set(configName, config);
        String def = instanceName + " = " + pyClassName + "(" + configName + ")";
        pyInterpreter.exec(def);
        
        Object impl = pyInterpreter.get(instanceName).__tojava__(AbstractBackend.class);
        return (AbstractBackend)impl;
    }

    /**
     * creates a backend implemented in Javascript.
     * @param jsPath location of javascript file. Starts with "js/"
     * @param config configuration
     * @throws ConfigError if there are any problems with the javascript.
     */
    public static IBackend createJavascriptBackend(String jsPath, HierarchicalConfiguration config) throws ConfigError
    {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("JavaScript");
        try {
            Object result = engine.eval(new InputStreamReader(getExternalResource(jsPath)));
            Invocable inv = (Invocable)engine;
            // init() js function mimics a java constructor.
            inv.invokeFunction("init", config);
            IBackend backend = inv.getInterface(IBackend.class);
            return backend;
        } catch (ScriptException ex) {
            throw new ConfigError(ex.getMessage(), ex);
        } catch (NoSuchMethodException ex) {
            throw new ConfigError(ex.getMessage(), ex);
        }
    }
    
    private static InputStream getExternalResource(String name) {
        // check all the search paths first.
        for (String path : backendSearchPaths) {
            File resource = new File(path, name);
            if (resource.exists()) {
                try {
                    return new FileInputStream(resource);
                } catch (FileNotFoundException suppress) {
                    // we already verified the file exists. so this would be bad.
                    throw new RuntimeException(suppress);
                }
            }
        }
        // now try loading from a few classloaders.
        return ExtBackendFactory.class.getClassLoader().getResourceAsStream(name);
    }
}
