#!/bin/bash

# exit on failure
set -e

agent=$1

uploadLibraryJar(){
    pattern=$1
    src="$SIMULATOR_HOME/lib/$pattern"
    rsync --checksum -avv -L -e "ssh $SSH_OPTIONS" $src $SIMULATOR_USER@$agent:hazelcast-simulator-$SIMULATOR_VERSION/lib
}

uploadToRemoteSimulatorDir(){
    src=$1
    target=$2
    rsync --checksum -avv -L -e "ssh $SSH_OPTIONS" $src $SIMULATOR_USER@$agent:hazelcast-simulator-$SIMULATOR_VERSION/$target
}

# purge Hazelcast JARs
ssh $SSH_OPTIONS $SIMULATOR_USER@$agent "rm -fr hazelcast-simulator-$SIMULATOR_VERSION/vendor-lib/ || true"
ssh $SSH_OPTIONS $SIMULATOR_USER@$agent "rm -fr hazelcast-simulator-$SIMULATOR_VERSION/user-lib/ || true"

ssh $SSH_OPTIONS $SIMULATOR_USER@$agent "mkdir -p hazelcast-simulator-$SIMULATOR_VERSION/lib/"
ssh $SSH_OPTIONS $SIMULATOR_USER@$agent "mkdir -p hazelcast-simulator-$SIMULATOR_VERSION/user-lib/"

#upload Simulator JARs
uploadLibraryJar "simulator-*"

# we don't copy all JARs to the agent to increase upload speed
#activemq libraries
uploadLibraryJar "activemq-core*"
uploadLibraryJar "geronimo-jms*"
uploadLibraryJar "geronimo-j2ee*"
uploadLibraryJar "slf4j-api*"

uploadLibraryJar "cache-api*"
uploadLibraryJar "commons-codec*"
uploadLibraryJar "commons-lang3*"
uploadLibraryJar "freemarker*"
uploadLibraryJar "gson-*"
uploadLibraryJar "HdrHistogram-*"
uploadLibraryJar "jopt*"
uploadLibraryJar "junit*"
uploadLibraryJar "log4j*"
uploadLibraryJar "slf4j-log4j12-*"

# hack to get hz enterprise working
if [ "$VENDOR" = "hazelcast-enterprise" ]; then
    uploadLibraryJar "netty-buffer-*"
    uploadLibraryJar "netty-codec-*"
    uploadLibraryJar "netty-common-*"
    uploadLibraryJar "netty-handler-*"
    uploadLibraryJar "netty-resolver-*"
    uploadLibraryJar "netty-tcnative-boringssl-static-*"
fi

# hack to get ignite working
if [ "$VENDOR" = "ignite" ]; then
    uploadLibraryJar "ignite-*"
    uploadLibraryJar "spring-*"
    uploadLibraryJar "commons-logging-*"
    uploadLibraryJar "h2-*"
fi

# hack to get infinispan working
if [ "$VENDOR" = "infinispan" ]; then
    uploadLibraryJar "infinispan-*"
    uploadLibraryJar "jboss*"
    uploadLibraryJar "jgroups*"
    uploadLibraryJar "netty*"
    uploadLibraryJar "jackson*"
    uploadLibraryJar "commons-pool*"
    uploadLibraryJar "javassist*"
    uploadLibraryJar "xstream*"
    uploadLibraryJar "xmlpull*"
    uploadLibraryJar "caffeine*"
    uploadLibraryJar "reactive-streams*"
    # specific version needed due to different versions
    uploadLibraryJar "rxjava-2.2.4*"
fi

# hack to get couchbase working
if [ "$VENDOR" = "couchbase" ]; then
    uploadLibraryJar "java-client-*"
    uploadLibraryJar "core-io-*"
    uploadLibraryJar "rxjava-*"
fi

#hack to get hz enterprise working
if [ "$VENDOR" = "jet" ]; then
    uploadLibraryJar "hazelcast-jet-*"
fi

# hack to get memcached working
if [ "$VENDOR" = "memcached" ]; then
    uploadLibraryJar "spymemcached*"
fi

# hack to get mongodb working
if [ "$VENDOR" = "mongodb" ]; then
    uploadLibraryJar "mongo-*"
fi

# hack to get redis working
if [ "$VENDOR" = "jedis" ]; then
    uploadLibraryJar "jedis*"
    uploadLibraryJar "commons-pool2*"
fi

# hack to get lettuce working
if [ "$VENDOR" = "lettuce" ]; then
    uploadLibraryJar "lettuce-core-*"
    uploadLibraryJar "netty-*"
    uploadLibraryJar "reactor-*"
    uploadLibraryJar "reactive-streams-*"
fi

# hack to get redisson working
if [ "$VENDOR" = "redisson" ]; then
    uploadLibraryJar "redisson-*"
    uploadLibraryJar "netty-*"
    uploadLibraryJar "reactor-*"
    uploadLibraryJar "reactive-streams-*"
    uploadLibraryJar "fst-*"
    uploadLibraryJar "jackson-*"
    uploadLibraryJar "jodd-*"
    uploadLibraryJar "objenesis-*"
fi

# upload remaining files
uploadToRemoteSimulatorDir "$SIMULATOR_HOME/bin/" "bin"
uploadToRemoteSimulatorDir "$SIMULATOR_HOME/conf/" "conf"
uploadToRemoteSimulatorDir "$SIMULATOR_HOME/jdk-install/" "jdk-install"
uploadToRemoteSimulatorDir "$SIMULATOR_HOME/test-lib/" "test-lib/"
uploadToRemoteSimulatorDir "$SIMULATOR_HOME/user-lib/" "user-lib/"
