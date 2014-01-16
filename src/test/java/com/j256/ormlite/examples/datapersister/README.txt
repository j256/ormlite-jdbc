This example shows how to define custom data persisters which tune how ORMLite writes and reads
data from the database.  The DateTimePersister can store org.joda.time.DateTime objects to the
database and MyDatePersister is used to tune how the database results are returned. 

This example depends on the H2 database which is a native Java SQL implementation.  You can
download the latest jar from the website:

    http://www.h2database.com/html/download.html

For more examples see: http://ormlite.com/docs/examples
