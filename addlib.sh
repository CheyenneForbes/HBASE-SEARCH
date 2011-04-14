mvn install:install-file -DgroupId=org.apache.lucene -DartifactId=lucene-core -Dversion=4.0-SNAPSHOT -Dpackaging=jar -Dfile=lucene/build/lucene-core-4.0-SNAPSHOT.jar
mvn install:install-file -DgroupId=org.apache.lucene -DartifactId=lucene-analyzers-common -Dversion=4.0-SNAPSHOT -Dpackaging=jar -Dfile=modules/analysis/build/common/lucene-analyzers-common-4.0-SNAPSHOT.jar
mvn install:install-file -DgroupId=org.apache.lucene -DartifactId=lucene-misc -Dversion=4.0-SNAPSHOT -Dpackaging=jar -Dfile=lucene/build/contrib/misc/lucene-misc-4.0-SNAPSHOT.jar
mvn install:install-file -DgroupId=org.apache.lucene -DartifactId=lucene-benchmark -Dversion=4.0-SNAPSHOT -Dpackaging=jar -Dfile=modules/benchmark/build/lucene-benchmark-4.0-SNAPSHOT.jar
