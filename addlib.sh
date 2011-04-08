mvn install:install-file -DgroupId=org.apache.lucene -DartifactId=lucene-core -Dversion=4.0-SNAPSHOT -Dpackaging=jar -Dfile=lib/lucene-core-4.0-SNAPSHOT.jar
mvn install:install-file -DgroupId=org.apache.lucene -DartifactId=lucene-analyzers-common -Dversion=4.0-SNAPSHOT -Dpackaging=jar -Dfile=lib/lucene-analyzers-common-4.0-SNAPSHOT.jar
mvn install:install-file -DgroupId=org.apache.lucene -DartifactId=lucene-misc -Dversion=4.0-SNAPSHOT -Dpackaging=jar -Dfile=lib/lucene-misc-4.0-SNAPSHOT.jar

