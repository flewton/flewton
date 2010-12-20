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
package com.rackspace.flewton;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import com.rackspace.flewton.backend.ExtBackendFactory;
import com.rackspace.flewton.backend.IBackend;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.HierarchicalINIConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.FixedReceiveBufferSizePredictorFactory;
import org.jboss.netty.channel.socket.DatagramChannelFactory;
import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rackspace.flewton.backend.AbstractBackend;

public class CollectorServer {
    private static final Logger logger = LoggerFactory.getLogger(CollectorServer.class);
    private static String[] configSearchPaths;
    
    static {
        // Each of these paths is searched in order when no configuration file
        // is specified.  The search stops when an existing config is found.
        configSearchPaths = new String[]{
                "/etc/flewton/flewton.cfg",
                "/etc/flewton.cfg",
                "flewton.cfg"
        };
    }
    
    private static List<IBackend> createBackends(String[] backendNames, HierarchicalINIConfiguration config) throws ConfigError {
        List<IBackend> backends = new ArrayList<IBackend>();
        
        for (String name : backendNames) {
            try {
                logger.info("Adding backend: {}", name);
                IBackend backend = null;
                SubnodeConfiguration subConfig = config.getSection(name.replace('.', '/'));
                if (name.startsWith("py/"))
                    backend = ExtBackendFactory.createPythonBackend(name, config);
                else if (name.startsWith("js/"))
                    backend = ExtBackendFactory.createJavascriptBackend(name, config);
                else {
                    Class<?> backendClass = Class.forName(name.replace('/', '.'));
                    backend = (AbstractBackend)backendClass.getConstructor(HierarchicalConfiguration.class).newInstance(subConfig);
                }
                backends.add(backend);
            } catch (ClassNotFoundException e) {
                logger.error("Backend not found: {} (not in classpath?)", name);
            } catch (InstantiationException e) {
                logger.error("Unable to instantiate backend of type: " + name, e);
            } catch (IllegalAccessException e) {
                logger.error("Error creating instance of " + name, e);
            } catch (IllegalArgumentException e) {
                logger.error("Error creating instance of " + name, e);
            } catch (SecurityException e) {
                logger.error("Error creating instance of " + name, e);
            } catch (InvocationTargetException e) {
                if (e.getCause() instanceof ConfigError)
                    throw (ConfigError)e.getCause();
                logger.error("Error creating instance of " + name, e);
            } catch (NoSuchMethodException e) {
                logger.error("Error creating instance of " + name, e);
            }
        }
        
        return backends;
    }
    
    private static HierarchicalINIConfiguration createConfig() throws ConfigurationException {
        File configFile = null;
        
        // Path specified using system property.
        if (System.getProperty("flewton.config") != null) {
            configFile = new File(System.getProperty("flewton.config"));
            
            if (!(configFile.exists())) {
                logger.error("Cannot load configuration: {} not found.", configFile.getAbsolutePath());
                throw new RuntimeException(configFile.getAbsolutePath() + " not found.");
            }
        // Search in candidate paths
        } else {
            for (String path : configSearchPaths) {
                configFile = new File(path);
                if (configFile.exists())
                    break;
            }
            
            // No configuration found.
            if (!(configFile.exists())) {
                StringBuilder pathStr = new StringBuilder();
                for (int i = 0; i < configSearchPaths.length; i++) {
                    pathStr.append(configSearchPaths[i]);
                    if (i < (configSearchPaths.length - 1))
                        pathStr.append(", ");
                }
                
                logger.error("No configuration found in search path: {}", pathStr.toString());
                throw new RuntimeException("No");
            }
        }
        
        return new HierarchicalINIConfiguration(configFile);
    }
    
    public static void main(String[] args) throws ConfigurationException, ConfigError {
        HierarchicalINIConfiguration config = createConfig();
        
        // UDP port number.
        int remotePort = config.getInt("listenPort", 9995);
        // Backend class names
        String[] backEnds = config.getStringArray("backendClass");

        if (backEnds.length > 0)
            CollectorHandler.setBackends(createBackends(backEnds, config));
        
        DatagramChannelFactory f = new NioDatagramChannelFactory(Executors.newCachedThreadPool());
        ConnectionlessBootstrap bootstrap = new ConnectionlessBootstrap(f);
        
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline(new CollectorHandler());
            }
        });
        
        // Disable broadcast
        bootstrap.setOption("broadcast", false);
        
        // Allow packets as large as 2048 bytes (default is 768)
        bootstrap.setOption("receiveBufferSizePredictorFactory",
                            new FixedReceiveBufferSizePredictorFactory(2048));

        logger.info("Binding to UDP 0.0.0.0:{}", remotePort);
        bootstrap.bind(new InetSocketAddress(remotePort));
    }

}
